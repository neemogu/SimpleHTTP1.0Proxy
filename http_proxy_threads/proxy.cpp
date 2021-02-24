#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <vector>
#include <netdb.h>
#include <csignal>
#include <cctype>
#include "picohttpparser.h"

#define NO_CACHE false

#define MAX_CACHED_RESPONSE_SIZE (150*1024*1024)
#define MAX_HEADERS 100
#define BACKLOG 10

#define TRANSFER_BUF_SIZE 16384

const char *http_scheme = "http://";
const char *http_server_error = "HTTP/1.0 500 Internal Server Error\r\n\r\n<html><head>Server Error</head><body></body></html>";
const char *http_service_unavailable = "HTTP/1.0 503 Service Unavailable\r\n\r\n<html><head>Service Unavailable</head><body></body></html>";
const char *http_version_unsupported = "HTTP/1.0 505 HTTP Version Not Supported\r\n\r\n<html><head>Version Not Supported</head><body></body></html>";
const char *http_length_required = "HTTP/1.0 411 Length Required\r\n\r\n<html><head>Length Required</head><body></body></html>";
const char *http_method_not_allowed = "HTTP/1.0 405 Method Not Allowed\r\nAllow: GET, HEAD, POST, DELETE, PUT\r\n\r\n<html><head>Method Not Allowed</head><body></body></html>";
const char *http_bad_request = "HTTP/1.0 400 Bad Request\r\n\r\n<html><head>Bad Request</head><body></body></html>";

bool is_interrupted = false;

void sigint_handler(int signum) {
    is_interrupted = true;
}

struct cache {
    std::string response;
    volatile bool is_error{};
    volatile bool is_full{};
    int clients_num{};
    pthread_cond_t write_condition;
    pthread_cond_t read_condition;
    int readers{};
    pthread_mutex_t mutex;
};

struct parsed_http {
    //request
    std::string method;
    std::string path;
    int version{};
    //response
    int status{};
    //general
    long content_length{-1};
};

struct http_buf {
    std::string buf;
    parsed_http parsed_buf;
    size_t bytes_sent{};
    volatile bool is_error{};
    struct cache *cache{nullptr}; // for caching mode
    bool cache_in_map{};
    pthread_mutex_t mutex;
    pthread_cond_t condition;
};

union thread_arg {
    struct client_arg {
        int client_fd;
    } c_arg;
    struct server_arg {
        int server_fd;
        http_buf *server_buf; // first lock
        http_buf *client_buf; // second lock
    } s_arg;
};

std::unordered_map<std::string, cache*> cache_map;
pthread_mutex_t cache_map_mutex = PTHREAD_MUTEX_INITIALIZER;

void lock_mutex(pthread_mutex_t *mutex) {
    int err = pthread_mutex_lock(mutex);
    if (err != 0) {
        fprintf(stderr, "mutex lock: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
}

void unlock_mutex(pthread_mutex_t *mutex) {
    int err = pthread_mutex_unlock(mutex);
    if (err != 0) {
        fprintf(stderr, "mutex unlock: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
}

void cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
    int err = pthread_cond_wait(cond, mutex);
    if (err != 0) {
        fprintf(stderr, "cond_wait: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
}

void cond_broadcast(pthread_cond_t *cond) {
    int err = pthread_cond_broadcast(cond);
    if (err != 0) {
        fprintf(stderr, "mutex unlock: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
}

void cache_read_lock(cache *c, size_t bytes_sent) {
    lock_mutex(&c->mutex);
    while(c->response.length() <= bytes_sent && !c->is_error && !c->is_full) {
        cond_wait(&c->read_condition, &c->mutex);
    }
    ++c->readers;
    unlock_mutex(&c->mutex);
}

void cache_read_unlock(cache *c) {
    lock_mutex(&c->mutex);
    if (--c->readers == 0) {
        cond_broadcast(&c->write_condition);
    }
    unlock_mutex(&c->mutex);
}

void cache_write_lock(cache *c) {
    lock_mutex(&c->mutex);
    while (c->readers) {
        cond_wait(&c->write_condition, &c->mutex);
    }
}

void cache_write_unlock(cache *c) {
    cond_broadcast(&c->read_condition);
    unlock_mutex(&c->mutex);
}

void free_cache_entry(cache *c) {
    int err = pthread_mutex_destroy(&c->mutex);
    if (err != 0) {
        fprintf(stderr, "mutex destroy: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
    err = pthread_cond_destroy(&c->read_condition);
    if (err != 0) {
        fprintf(stderr, "condition destroy: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
    err = pthread_cond_destroy(&c->write_condition);
    if (err != 0) {
        fprintf(stderr, "condition destroy: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
    delete c;
}

bool clean_cache_map() {
    int deleted = 0;
    for (auto it = cache_map.cbegin(); it != cache_map.cend();) {
        if (it->second->clients_num == 0 && it->second->is_full) {
            free_cache_entry(it->second);
            it = cache_map.erase(it);
            ++deleted;
        } else {
            ++it;
        }
    }
    return deleted;
}

bool init_cache_entry(cache *c) {
    int err = pthread_mutex_init(&c->mutex, nullptr);
    if (err != 0) {
        fprintf(stderr, "mutex init: %s", strerror(err));
        return false;
    }
    err = pthread_cond_init(&c->read_condition, nullptr);
    if (err != 0) {
        pthread_mutex_destroy(&c->mutex);
        fprintf(stderr, "condition init: %s", strerror(err));
        return false;
    }
    err = pthread_cond_init(&c->write_condition, nullptr);
    if (err != 0) {
        pthread_mutex_destroy(&c->mutex);
        pthread_cond_destroy(&c->read_condition);
        fprintf(stderr, "condition init: %s", strerror(err));
        return false;
    }
    return true;
}

void create_cache_entry(cache **new_cache_entry) {
    try {
        *new_cache_entry = new cache;
    } catch (std::bad_alloc &) {
        lock_mutex(&cache_map_mutex);
        clean_cache_map();
        unlock_mutex(&cache_map_mutex);
        try {
            *new_cache_entry = new cache;
        } catch (std::bad_alloc &) {
            *new_cache_entry = nullptr;
            return;
        }
    }
    if (!init_cache_entry(*new_cache_entry)) {
        delete *new_cache_entry;
        *new_cache_entry = nullptr;
    }
}

bool cache_map_insert(const std::string &request, cache *c) {
    try {
        cache_map.insert({request, c});
    } catch (std::bad_alloc &) {
        clean_cache_map();
        try {
            cache_map.insert({request, c});
        } catch(std::bad_alloc &) {
            return false;
        }
    }
    return true;
}

bool init_http_buf(http_buf *buf) {
    int err = pthread_mutex_init(&buf->mutex, nullptr);
    if (err != 0) {
        fprintf(stderr, "mutex init: %s", strerror(err));
        return false;
    }
    err = pthread_cond_init(&buf->condition, nullptr);
    if (err != 0) {
        pthread_mutex_destroy(&buf->mutex);
        fprintf(stderr, "condition init: %s", strerror(err));
        return false;
    }
    return true;
}

void create_http_buf(http_buf **new_http_buf) {
    try {
        *new_http_buf = new http_buf;
    } catch (std::bad_alloc &) {
        lock_mutex(&cache_map_mutex);
        clean_cache_map();
        unlock_mutex(&cache_map_mutex);
        try {
            *new_http_buf = new http_buf;
        } catch (std::bad_alloc &) {
            *new_http_buf = nullptr;
            return;
        }
    }
    if (!init_http_buf(*new_http_buf)) {
        delete *new_http_buf;
        *new_http_buf = nullptr;
    }
}

void free_http_buf(http_buf *buf) {
    int err = pthread_mutex_destroy(&buf->mutex);
    if (err != 0) {
        fprintf(stderr, "mutex destroy: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
    err = pthread_cond_destroy(&buf->condition);
    if (err != 0) {
        fprintf(stderr, "condition destroy: %s", strerror(err));
        exit(EXIT_FAILURE);
    }
    delete buf;
}

thread_arg *create_thread_arg() {
    thread_arg *ta;
    try {
        ta = new thread_arg;
    } catch (std::bad_alloc &) {
        lock_mutex(&cache_map_mutex);
        clean_cache_map();
        unlock_mutex(&cache_map_mutex);
        try {
            ta = new thread_arg;
        } catch (std::bad_alloc &) {
            return nullptr;
        }
    }
    return ta;
}

bool create_new_thread(void *(*routine)(void *), thread_arg *arg) {
    pthread_t new_thread;
    int err = pthread_create(&new_thread, nullptr, routine, arg);
    if (err != 0) {
        fprintf(stderr, "Can't create new thread: %s\n", strerror(err));
        return false;
    }
    pthread_detach(new_thread);
    return true;
}

void *client_routine(void *arg);
void *server_routine(void *arg);

void create_client_thread(int client_fd) {
    thread_arg *arg = create_thread_arg();
    if (!arg) {
        fprintf(stderr, "Can't create new client thread: not enough memory\n");
        return;
    }
    arg->c_arg.client_fd = client_fd;
    create_new_thread(client_routine, arg);
}

bool create_server_thread(int server_fd, int client_fd, http_buf *client_buf, http_buf *server_buf) {
    thread_arg *arg = create_thread_arg();
    if (!arg) {
        return false;
    }
    arg->s_arg.server_fd = server_fd;
    arg->s_arg.client_buf = client_buf;
    arg->s_arg.server_buf = server_buf;
    return create_new_thread(server_routine, arg);
}

int connect_to_server(const std::string &hostname) {
    printf("Trying to connect to %s\n", hostname.c_str());
    int server_fd;
    struct addrinfo hints{}, *server_info;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int err;
    if ((err = getaddrinfo(hostname.c_str(), "http", &hints, &server_info)) < 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return -1;
    }
    struct addrinfo *p = server_info;
    for (; p != nullptr; p = p->ai_next) {
        if ((server_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0) {
            perror("create socket to connect");
            continue;
        }
        if (connect(server_fd, server_info->ai_addr, server_info->ai_addrlen) < 0) {
            perror("connect to server");
            close(server_fd);
            continue;
        }
        break;
    }
    if (!p) {
        fprintf(stderr, "Can't connect to %s", hostname.c_str());
        freeaddrinfo(server_info);
        return -1;
    }
    return server_fd;
}

bool is_get_or_head_method(const char *method) {
    return !strcasecmp(method, "GET") || !strcasecmp(method, "HEAD");
}

bool is_supported_method(const char *method) {
    return is_get_or_head_method(method) || !strcasecmp(method, "PUT") || !strcasecmp(method, "POST") ||
    !strcasecmp(method, "DELETE");
}

bool is_version_supported(int minor_version) {
    return minor_version == 0 || minor_version == 1;
}

bool str_append(std::string &str, const char *buf, size_t bytes_num) {
    try {
        str.append(buf, bytes_num);
    } catch (std::bad_alloc &) {
        lock_mutex(&cache_map_mutex);
        clean_cache_map();
        unlock_mutex(&cache_map_mutex);
        try {
            str.append(buf, bytes_num);
        } catch (std::bad_alloc &) {
            return false;
        }
    }
    return true;
}

bool str_insert(std::string &str1, const std::string &str2, size_t start_pos) {
    try {
        str1.insert(start_pos, str2);
    } catch (std::bad_alloc &) {
        lock_mutex(&cache_map_mutex);
        clean_cache_map();
        unlock_mutex(&cache_map_mutex);
        try {
            str1.insert(start_pos, str2);
        } catch (std::bad_alloc &) {
            return false;
        }
    }
    return true;
}

// result hostname will be in path string
// hostname will be erased from request string
bool extract_hostname_from_request(std::string &request, std::string &path, size_t method_len) {
    std::string::size_type slash_pos = path.find_first_of('/');
    if (slash_pos != std::string::npos) {
        path.erase(slash_pos);
        path.shrink_to_fit();
        request.erase(method_len + 1, path.length());
        request.shrink_to_fit();
        return true;
    } else {
        return false;
    }
}

bool is_content_length_header(const char *name, int name_len) {
    const char *content_length = "Content-Length";
    if (name_len != strlen(content_length)) {
        return false;
    }
    return strncmp(name, content_length, name_len) == 0;
}

long str_to_long(const char *str, int strlen) {
    long res = 0;
    for (int i = 0; i < strlen; ++i) {
        if (!std::isdigit(str[i])) {
            return -1;
        }
        res *= 10;
        res += str[i] - '0';
    }
    return res;
}

int parse_full_http_request(http_buf &buf) {
    const char *method;
    const char *path;
    size_t method_len;
    size_t path_len;
    int version;
    struct phr_header headers[MAX_HEADERS];
    size_t num_headers = sizeof(headers) / sizeof(headers[0]);
    int res = phr_parse_request(buf.buf.c_str(), buf.buf.length(), &method, &method_len, &path,
                                &path_len, &version, headers, &num_headers, 0);
    if (res >= 0) {
        long long content_length = -1;
        for (int i = 0; i < num_headers; ++i) {
            if (is_content_length_header(headers[i].name, headers[i].name_len)) {
                content_length = str_to_long(headers[i].value, headers[i].value_len);
                break;
            }
        }
        buf.parsed_buf.version = version;
        buf.parsed_buf.content_length = content_length;
        try {
            buf.parsed_buf.method.append(method, method_len);
            buf.parsed_buf.path.append(path, path_len);
        } catch (std::bad_alloc &) {
            lock_mutex(&cache_map_mutex);
            clean_cache_map();
            unlock_mutex(&cache_map_mutex);
            try {
                if (buf.parsed_buf.method.compare(0, method_len, method, method_len) != 0) {
                    buf.parsed_buf.method.append(method, method_len);
                }
                buf.parsed_buf.path.append(path, path_len);
            } catch(std::bad_alloc &) {
                return -3;
            }
        }
    }
    return res;
}

int parse_full_http_response(const std::string &response, parsed_http &parsed_buf) {
    const char *msg;
    size_t msg_len;
    int version;
    int status;
    struct phr_header headers[MAX_HEADERS];
    size_t num_headers = sizeof(headers) / sizeof(headers[0]);
    int res = phr_parse_response(response.c_str(), response.length(), &version, &status, &msg, &msg_len,
                                 headers, &num_headers, 0);
    if (res >= 0) {
        long long content_length = -1;
        for (int i = 0; i < num_headers; ++i) {
            if (is_content_length_header(headers[i].name, headers[i].name_len)) {
                if (!NO_CACHE) {
                    content_length = str_to_long(headers[i].value, headers[i].value_len);
                }
                break;
            }
        }
        parsed_buf.content_length = content_length;
        parsed_buf.status = status;
    }
    return res;
}

int send_data(int fd, const char *buf, size_t buf_len) {
    int send_res = send(fd, buf, buf_len, MSG_NOSIGNAL | MSG_DONTWAIT);
    if (send_res < 0) {
        if (errno == ENOMEM) {
            lock_mutex(&cache_map_mutex);
            clean_cache_map();
            unlock_mutex(&cache_map_mutex);
            return 0;
        } else if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        } else {
            perror("send");
        }
    }
    return send_res;
}

int recv_data(int fd, char *buf, size_t buf_len) {
    int recv_res;
    for (;;) {
        recv_res = recv(fd, buf, buf_len, 0);
        if (recv_res < 0) {
            if (errno == EINTR) {
                if (!is_interrupted) {
                    continue;
                }
            } else {
                perror("recv");
            }
        }
        break;
    }
    return recv_res;
}

void server_delete_cache(cache *c) {
    if (c) {
        lock_mutex(&cache_map_mutex);
        if (c->clients_num > 0) {
            cache_write_lock(c);
            c->is_error = true;
            cache_write_unlock(c);
        } else {
            for (auto it = cache_map.begin(); it != cache_map.end(); ++it) {
                if (it->second == c) {
                    cache_map.erase(it);
                    break;
                }
            }
            free_cache_entry(c);
        }
        unlock_mutex(&cache_map_mutex);
    }
}

// pointer to cache struct should be perceived as invalid after this function call
void unsub_from_cache(cache *c) {
    if (c) {
        lock_mutex(&cache_map_mutex);
        if (--c->clients_num == 0 && c->is_error) {
            for (auto it = cache_map.begin(); it != cache_map.end(); ++it) {
                if (it->second == c) {
                    cache_map.erase(it);
                    free_cache_entry(c);
                    break;
                }
            }
        }
        unlock_mutex(&cache_map_mutex);
    }
}

void reset_client_connection(int client_fd, http_buf *server_buf, http_buf *client_buf) {
    if (!server_buf) {
        free_http_buf(client_buf);
        close(client_fd);
        return;
    }
    lock_mutex(&server_buf->mutex);
    lock_mutex(&client_buf->mutex);
    if (server_buf->is_error) {
        unlock_mutex(&server_buf->mutex);
        unlock_mutex(&client_buf->mutex);
        free_http_buf(client_buf);
        free_http_buf(server_buf);
    } else {
        client_buf->is_error = true;
        cond_broadcast(&server_buf->condition);
        unlock_mutex(&server_buf->mutex);
        unlock_mutex(&client_buf->mutex);
    }
    close(client_fd);
}

void reset_server_connection(int server_fd, http_buf *server_buf, http_buf *client_buf) {
    lock_mutex(&server_buf->mutex);
    lock_mutex(&client_buf->mutex);
    if (client_buf->is_error) {
        unlock_mutex(&server_buf->mutex);
        unlock_mutex(&client_buf->mutex);
        free_http_buf(client_buf);
        free_http_buf(server_buf);
    } else {
        server_buf->is_error = true;
        cond_broadcast(&client_buf->condition);
        unlock_mutex(&server_buf->mutex);
        unlock_mutex(&client_buf->mutex);
    }
    close(server_fd);
}

bool client_return_error_to_client(int client_fd, http_buf *client_buf, http_buf *server_buf, const char *http_error) {
    client_buf->buf.clear();
    if (!str_append(client_buf->buf, http_error, strlen(http_error))) {
        reset_client_connection(client_fd, server_buf, client_buf);
        return false;
    }
    client_buf->buf.shrink_to_fit();
    unsub_from_cache(client_buf->cache);
    client_buf->cache = nullptr;
    client_buf->bytes_sent = 0;
    client_buf->parsed_buf.content_length = strlen(http_error);
    return true;
}

void server_return_error_to_client(http_buf *client_buf, const char *http_error) {
    lock_mutex(&client_buf->mutex);
    client_buf->buf.clear();
    str_append(client_buf->buf, http_error, strlen(http_error));
    client_buf->buf.shrink_to_fit();
    client_buf->parsed_buf.content_length = client_buf->buf.length();
    client_buf->bytes_sent = 0;
    unlock_mutex(&client_buf->mutex);
}

void server_error(int server_fd, http_buf *server_buf, http_buf *client_buf, const char *http_error) {
    server_delete_cache(server_buf->cache);
    reset_server_connection(server_fd, server_buf, client_buf);
    server_return_error_to_client(client_buf, http_error);
}

void create_server_send(http_buf *buf, int client_fd, bool caching, long request_head_length, http_buf **server_buf) {
    if (!extract_hostname_from_request(buf->buf, buf->parsed_buf.path, buf->parsed_buf.method.length())) {
        client_return_error_to_client(client_fd, buf, nullptr, http_bad_request);
        return;
    }
    int server_fd = connect_to_server(buf->parsed_buf.path);
    if (server_fd == -1) {
        client_return_error_to_client(client_fd, buf, nullptr, http_server_error);
        return;
    }
    http_buf *serv_buf;
    create_http_buf(&serv_buf);
    if (!serv_buf) {
        close(server_fd);
        client_return_error_to_client(client_fd, buf, nullptr, http_server_error);
        return;
    }
    if (caching) {
        create_cache_entry(&serv_buf->cache);
    }
    if (request_head_length != -1) {
        buf->bytes_sent = buf->buf.length();
        buf->parsed_buf.content_length += request_head_length;
        buf->parsed_buf.content_length -= buf->parsed_buf.path.length(); // minus hostname length
        serv_buf->parsed_buf.content_length = buf->parsed_buf.content_length;
    } else {
        buf->parsed_buf.content_length = -1;
        serv_buf->parsed_buf.content_length = -1;
    }
    serv_buf->parsed_buf.method = std::move(buf->parsed_buf.method);
    serv_buf->parsed_buf.path = std::move(buf->parsed_buf.path);
    serv_buf->buf.swap(buf->buf);
    if (!create_server_thread(server_fd, client_fd, buf, serv_buf)) {
        close(server_fd);
        server_delete_cache(serv_buf->cache);
        free_http_buf(serv_buf);
        client_return_error_to_client(client_fd, buf, nullptr, http_server_error);
        return;
    }
    *server_buf = serv_buf;
}

bool client_receive(int client_fd, http_buf *client_buf, http_buf **server_buf) {
    *server_buf = nullptr;
    char transfer_buf[TRANSFER_BUF_SIZE];
    for (;;) {
        if (is_interrupted) {
            reset_client_connection(client_fd, *server_buf, client_buf);
            return false;
        }
        int received = recv_data(client_fd, transfer_buf, TRANSFER_BUF_SIZE);
        if (received < 0) {
            reset_client_connection(client_fd, *server_buf, client_buf);
            return false;
        }
        if (*server_buf) { // sending put/post/delete content
            http_buf *serv_buf = *server_buf;
            lock_mutex(&serv_buf->mutex);
            if (serv_buf->is_error) {
                unlock_mutex(&serv_buf->mutex);
                return true;
            }
            lock_mutex(&client_buf->mutex);
            if (!str_append(serv_buf->buf, transfer_buf, received)) {
                unlock_mutex(&serv_buf->mutex);
                unlock_mutex(&client_buf->mutex);
                return client_return_error_to_client(client_fd, client_buf, serv_buf, http_server_error);
            }
            cond_broadcast(&serv_buf->condition);
            unlock_mutex(&serv_buf->mutex);
            client_buf->bytes_sent += received;
            if (client_buf->bytes_sent >= client_buf->parsed_buf.content_length) {
                client_buf->bytes_sent = 0;
                client_buf->parsed_buf.content_length = -1;
                unlock_mutex(&client_buf->mutex);
                return true;
            } else if (received == 0) {
                unlock_mutex(&client_buf->mutex);
                return client_return_error_to_client(client_fd, client_buf, serv_buf, http_bad_request);
            }
            unlock_mutex(&client_buf->mutex);
            continue;
        }
        if (!str_append(client_buf->buf, transfer_buf, received)) {
            return client_return_error_to_client(client_fd, client_buf, nullptr, http_server_error);
        }
        //printf("%s\n", client_buf.client_buf.c_str());
        int parse_res = parse_full_http_request(*client_buf);
        if (parse_res == -1 || (parse_res == -2 && received == 0)) {
            return client_return_error_to_client(client_fd, client_buf, nullptr, http_bad_request);
        } else if (parse_res == -3) {
            return client_return_error_to_client(client_fd, client_buf, nullptr, http_server_error);
        } else if (parse_res >= 0) {
            if (!is_supported_method(client_buf->parsed_buf.method.c_str())) {
                return client_return_error_to_client(client_fd, client_buf, nullptr, http_method_not_allowed);
            }
            if (!is_version_supported(client_buf->parsed_buf.version)) {
                return client_return_error_to_client(client_fd, client_buf, nullptr, http_version_unsupported);
            }
            std::string::size_type scheme_in_path = client_buf->parsed_buf.path.find(http_scheme);
            std::string::size_type scheme_in_request = client_buf->buf.find(http_scheme);
            if (scheme_in_path != std::string::npos) {
                client_buf->parsed_buf.path.erase(scheme_in_path, strlen(http_scheme));
            }
            if (scheme_in_request != std::string::npos) {
                client_buf->buf.erase(scheme_in_request, strlen(http_scheme));
            }
            if (is_get_or_head_method(client_buf->parsed_buf.method.c_str())) {
                lock_mutex(&cache_map_mutex);
                auto found_cache_entry = cache_map.find(client_buf->buf);
                if (found_cache_entry != cache_map.end()) {
                    client_buf->cache = found_cache_entry->second;
                    ++client_buf->cache->clients_num;
                    unlock_mutex(&cache_map_mutex);
                    client_buf->buf.clear();
                    client_buf->buf.shrink_to_fit();
                } else {
                    unlock_mutex(&cache_map_mutex);
                    create_server_send(client_buf, client_fd, true, -1, server_buf);
                }
                return true;
            } else {
                if (client_buf->parsed_buf.content_length == -1) {
                    return client_return_error_to_client(client_fd, client_buf, *server_buf, http_length_required);
                } else { // start sending put/post/delete content
                    create_server_send(client_buf, client_fd, false, parse_res, server_buf);
                }
            }
        }
    }
}

void wait_send(int fd) {
    struct pollfd poll_req[1];
    poll_req[0].events = POLLOUT;
    poll_req[0].fd = fd;
    poll(poll_req, 1, -1);
}

bool server_send(int server_fd, http_buf *client_buf, http_buf *server_buf) {
    for (;;) {
        if (is_interrupted) {
            reset_server_connection(server_fd, server_buf, client_buf);
            return false;
        }
        if (client_buf->is_error) {
            reset_server_connection(server_fd, server_buf, client_buf);
            return false;
        }
        lock_mutex(&server_buf->mutex);
        if (server_buf->parsed_buf.content_length != -1) { // request has a content
            int sent = send_data(server_fd, server_buf->buf.c_str(), server_buf->buf.length());
            if (sent == 0) {
                unlock_mutex(&server_buf->mutex);
                wait_send(server_fd);
                continue;
            }
            if (sent < 0) {
                unlock_mutex(&server_buf->mutex);
                server_error(server_fd, server_buf, client_buf, http_service_unavailable);
                return false;
            }
            server_buf->bytes_sent += sent;
            server_buf->buf.erase(0, sent);
            if (server_buf->bytes_sent >= server_buf->parsed_buf.content_length) {
                server_buf->buf.shrink_to_fit();
                unlock_mutex(&server_buf->mutex);
                return true;
            } else if (server_buf->buf.empty()) {
                while(server_buf->buf.empty() && !client_buf->is_error) {
                    cond_wait(&server_buf->condition, &server_buf->mutex);
                }
                unlock_mutex(&server_buf->mutex);
            } else {
                unlock_mutex(&server_buf->mutex);
            }
        } else { // request doesn't have a content
            int sent = send_data(server_fd, server_buf->buf.c_str() + server_buf->bytes_sent, server_buf->buf.length() - server_buf->bytes_sent);
            if (sent == 0) {
                unlock_mutex(&server_buf->mutex);
                wait_send(server_fd);
                continue;
            }
            if (sent < 0) {
                unlock_mutex(&server_buf->mutex);
                server_error(server_fd, server_buf, client_buf, http_service_unavailable);
                return false;
            }
            server_buf->bytes_sent += sent;
            if (server_buf->bytes_sent >= server_buf->buf.length()) {
                if (!str_insert(server_buf->buf, server_buf->parsed_buf.path, server_buf->parsed_buf.method.length() + 1)) {
                    unlock_mutex(&server_buf->mutex);
                    server_error(server_fd, server_buf, client_buf, http_server_error);
                    return false;
                }
                unlock_mutex(&server_buf->mutex);
                return true;
            }
            unlock_mutex(&server_buf->mutex);
        }
    }
}

void server_receive(int server_fd, http_buf *client_buf, http_buf *server_buf) {
    char transfer_buf[TRANSFER_BUF_SIZE];
    for (;;) {
        if (is_interrupted) {
            reset_server_connection(server_fd, server_buf, client_buf);
            return;
        }
        int received = recv_data(server_fd,transfer_buf, TRANSFER_BUF_SIZE);
        if (received < 0) {
            server_error(server_fd, server_buf, client_buf, http_service_unavailable);
            return;
        }
        if (server_buf->cache) { // caching mode, buf.buf contains get or head request
            cache_write_lock(server_buf->cache);
            if (!str_append(server_buf->cache->response, transfer_buf, received)) {
                cache_write_unlock(server_buf->cache);
                server_error(server_fd, server_buf, client_buf, http_server_error);
                return;
            }
            cache_write_unlock(server_buf->cache);
            if (server_buf->cache_in_map) {
                lock_mutex(&cache_map_mutex);
                if (server_buf->cache->clients_num == 0) {
                    unlock_mutex(&cache_map_mutex);
                    server_delete_cache(server_buf->cache);
                    reset_server_connection(server_fd, server_buf, client_buf);
                    return;
                }
                unlock_mutex(&cache_map_mutex);
                if (server_buf->cache->response.length() >= server_buf->parsed_buf.content_length) {
                    cache_write_lock(server_buf->cache);
                    server_buf->cache->is_full = true;
                    cache_write_unlock(server_buf->cache);
                    reset_server_connection(server_fd, server_buf, client_buf);
                    return;
                }
            } else {
                lock_mutex(&cache_map_mutex);
                auto cache_map_found_record = cache_map.find(server_buf->buf);
                if (cache_map_found_record != cache_map.end()) { // response has been completely parsed
                    if (cache_map_found_record->second != server_buf->cache) {
                        ++cache_map_found_record->second->clients_num;
                        lock_mutex(&client_buf->mutex);
                        client_buf->cache = cache_map_found_record->second;
                        unlock_mutex(&cache_map_mutex);
                        cond_broadcast(&client_buf->condition);
                        unlock_mutex(&client_buf->mutex);
                        server_delete_cache(server_buf->cache);
                        reset_server_connection(server_fd, server_buf, client_buf);
                        return;
                    }
                    unlock_mutex(&cache_map_mutex);
                    server_buf->cache_in_map = true;
                } else {
                    unlock_mutex(&cache_map_mutex);
                    int parse_res = parse_full_http_response(server_buf->cache->response, server_buf->parsed_buf);
                    if (parse_res == -1 || (parse_res == -2 && received == 0)) {
                        server_error(server_fd, server_buf, client_buf, http_server_error);
                        return;
                    } else if (parse_res >= 0) {
                        lock_mutex(&client_buf->mutex);
                        if (server_buf->parsed_buf.status == 200 && server_buf->parsed_buf.content_length != -1 &&
                            server_buf->parsed_buf.content_length <= MAX_CACHED_RESPONSE_SIZE) {
                            lock_mutex(&cache_map_mutex);
                            if (!cache_map_insert(server_buf->buf, server_buf->cache)) {
                                server_error(server_fd, server_buf, client_buf, http_server_error);
                                return;
                            }
                            ++server_buf->cache->clients_num;
                            unlock_mutex(&cache_map_mutex);
                            client_buf->cache = server_buf->cache;
                            server_buf->parsed_buf.content_length += parse_res;
                        } else {
                            client_buf->buf.swap(server_buf->cache->response);
                            client_buf->parsed_buf.content_length = client_buf->buf.length() + 1;
                            server_delete_cache(server_buf->cache);
                            server_buf->cache = nullptr;
                        }
                        cond_broadcast(&client_buf->condition);
                        unlock_mutex(&client_buf->mutex);
                    }
                }
            }
        } else { // through mode
            lock_mutex(&client_buf->mutex);
            if (client_buf->is_error) {
                unlock_mutex(&client_buf->mutex);
                reset_server_connection(server_fd, server_buf, client_buf);
                return;
            }
            if (!str_append(client_buf->buf, transfer_buf, received)) {
                unlock_mutex(&client_buf->mutex);
                server_error(server_fd, server_buf, client_buf, http_server_error);
                return;
            }
            if (client_buf->parsed_buf.content_length == -1) {
                client_buf->parsed_buf.content_length = received + 1;
            } else {
                client_buf->parsed_buf.content_length += received;
            }
            cond_broadcast(&client_buf->condition);
            if (received == 0) {
                client_buf->parsed_buf.content_length -= 1;
                unlock_mutex(&client_buf->mutex);
                reset_server_connection(server_fd, server_buf, client_buf);
                return;
            }
            unlock_mutex(&client_buf->mutex);
        }
    }
}

void client_send(int client_fd, http_buf *client_buf, http_buf *server_buf) {
    for (;;) {
        if (is_interrupted) {
            reset_client_connection(client_fd, server_buf, client_buf);
            return;
        }
        lock_mutex(&client_buf->mutex);
        if (client_buf->cache) { // cache mode
            cache_read_lock(client_buf->cache, client_buf->bytes_sent);
            if (client_buf->cache->is_error) {
                cache_read_unlock(client_buf->cache);
                unlock_mutex(&client_buf->mutex);
                unsub_from_cache(client_buf->cache);
                reset_client_connection(client_fd, server_buf, client_buf);
                return;
            }
            int sent = send_data(client_fd, client_buf->cache->response.c_str() + client_buf->bytes_sent,
                                 client_buf->cache->response.length() - client_buf->bytes_sent);
            if (sent == 0) {
                cache_read_unlock(client_buf->cache);
                unlock_mutex(&client_buf->mutex);
                wait_send(client_fd);
                continue;
            }
            if (sent < 0) {
                cache_read_unlock(client_buf->cache);
                unsub_from_cache(client_buf->cache);
                unlock_mutex(&client_buf->mutex);
                reset_client_connection(client_fd, server_buf, client_buf);
                return;
            }
            client_buf->bytes_sent += sent;
            if (client_buf->cache->response.length() <= client_buf->bytes_sent && client_buf->cache->is_full) {
                cache_read_unlock(client_buf->cache);
                unsub_from_cache(client_buf->cache);
                unlock_mutex(&client_buf->mutex);
                reset_client_connection(client_fd, server_buf, client_buf);
                return;
            }
            cache_read_unlock(client_buf->cache);
            unlock_mutex(&client_buf->mutex);
        }
        else { // through mode
            if (client_buf->buf.empty()) {
                while (!client_buf->cache && client_buf->buf.empty()) {
                    cond_wait(&client_buf->condition, &client_buf->mutex);
                }
                unlock_mutex(&client_buf->mutex);
                continue;
            }
            int sent = send_data(client_fd, client_buf->buf.c_str(), client_buf->buf.length());
            if (sent == 0) {
                unlock_mutex(&client_buf->mutex);
                wait_send(client_fd);
                continue;
            }
            if (sent < 0) {
                unlock_mutex(&client_buf->mutex);
                reset_client_connection(client_fd, server_buf, client_buf);
                return;
            }
            client_buf->bytes_sent += sent;
            client_buf->buf.erase(0, sent);
            if (client_buf->bytes_sent >= client_buf->parsed_buf.content_length) {
                unlock_mutex(&client_buf->mutex);
                reset_client_connection(client_fd, server_buf, client_buf);
                return;
            } else if (client_buf->buf.empty()) {
                while (client_buf->buf.empty() && client_buf->bytes_sent < client_buf->parsed_buf.content_length) {
                    cond_wait(&client_buf->condition, &client_buf->mutex);
                }
                unlock_mutex(&client_buf->mutex);
                if (client_buf->bytes_sent >= client_buf->parsed_buf.content_length) {
                    reset_client_connection(client_fd, server_buf, client_buf);
                    return;
                }
            } else {
                unlock_mutex(&client_buf->mutex);
            }
        }
    }
}

void *client_routine(void *arg) {
    auto *t_arg = (thread_arg *)arg;
    int client_fd = t_arg->c_arg.client_fd;
    http_buf *client_buf;
    create_http_buf(&client_buf);
    if (!client_buf) {
        return nullptr;
    }
    http_buf *server_buf;
    if (client_receive(client_fd, client_buf, &server_buf)) {
        client_send(client_fd, client_buf, server_buf);
    }
    return nullptr;
}

void *server_routine(void *arg) {
    auto *t_arg = (thread_arg *)arg;
    int server_fd = t_arg->s_arg.server_fd;
    http_buf *server_buf = t_arg->s_arg.server_buf;
    http_buf *client_buf = t_arg->s_arg.client_buf;
    if (server_send(server_fd, client_buf, server_buf)) {
        server_receive(server_fd, client_buf, server_buf);
    }
    return nullptr;
}

int create_listen_socket(const char *port) {
    int listen_fd;
    struct addrinfo hints{}, *listen_info;
    memset(&hints, 0, sizeof hints);
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int err;
    if ((err = getaddrinfo(nullptr, port, &hints, &listen_info)) < 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        exit(EXIT_FAILURE);
    }
    //char yes = '1';
    int yes = 1;
    struct addrinfo *p = listen_info;
    for (; p; p= p->ai_next) {
        if ((listen_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) < 0) {
            perror("create listening socket");
            continue;
        }
        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
            close(listen_fd);
            perror("setsockopt");
            exit(EXIT_FAILURE);
        }
        if (bind(listen_fd, p->ai_addr, p->ai_addrlen) < 0) {
            close(listen_fd);
            perror("bind");
            continue;
        }
        break;
    }
    freeaddrinfo(listen_info);
    if (!p) {
        fprintf(stderr, "can't create any listening socket");
        exit(EXIT_FAILURE);
    }
    if (listen(listen_fd, BACKLOG) < 0) {
        close(listen_fd);
        perror("listen");
        exit(EXIT_FAILURE);
    }
    return listen_fd;
}

int get_new_client(int listen_fd) {
    struct sockaddr_in client_addr{};
    socklen_t client_addr_size = sizeof client_addr;
    int new_client = accept(listen_fd, (struct sockaddr *)&client_addr, &client_addr_size);
    if (new_client < 0 && errno != EINTR) {
        perror("accepting new client");
    } else if (new_client >= 0) {
        printf("accepted new client\n");
    }
    return new_client;
}

void listen_connection(int listen_fd) {
    int new_client = get_new_client(listen_fd);
    if (new_client < 0) {
        return;
    }
    create_client_thread(new_client);
}

void set_sig_action(int signum, void (*handler)(int)) {
    struct sigaction action{};
    if (sigaction(signum, nullptr, &action) < 0) {
        perror("sigaction");
        exit(EXIT_FAILURE);
    }
    action.sa_handler = handler;
    if (sigaction(signum, &action, nullptr) < 0) {
        perror("sigaction");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        printf("Usage: <name> <port>");
        exit(EXIT_SUCCESS);
    }
    set_sig_action(SIGINT, sigint_handler);
    int listen_fd = create_listen_socket(argv[1]);
    printf("Started...\n");
    for (;;) {
        if (is_interrupted) {
            break;
        }
        listen_connection(listen_fd);
    }
    pthread_exit(nullptr);
}
