#include <iostream>
#include <poll.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <set>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <vector>
#include <netdb.h>
#include <csignal>
#include <cctype>
#include "picohttpparser.h"

#define NO_CACHE false

#define MAX_CACHED_RESPONSE_SIZE 51*1024*1024
#define MAX_HEADERS 100
#define BACKLOG 10
#define POLL_REQUESTS_START_SIZE 1021

#define TRANSFER_BUF_SIZE 8192
char transfer_buf[TRANSFER_BUF_SIZE];

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

enum http_buf_type {
    CLIENT_SEND, CLIENT_RECV, SERVER_SEND, SERVER_RECV
};

struct cache {
    std::string response;
    bool is_error{};
    bool is_full{};
    std::set<int> waiting_clients;
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
    http_buf() = default;
    http_buf(http_buf_type type) : type(type) {}
    std::string buf;
    parsed_http parsed_buf;
    size_t bytes_sent{};
    http_buf_type type{CLIENT_RECV};
    int opposite_fd{-1}; // for through mode
    struct cache *cache{nullptr}; // for caching mode
};

struct poll_requests {
    struct pollfd *requests;
    size_t count;
    size_t size;
    int listen_fd;
};

std::unordered_map<std::string, cache*> cache_map;
std::unordered_map<int, http_buf> buf_map;

bool clean_cache_map() {
    int deleted = 0;
    for (auto it = cache_map.cbegin(); it != cache_map.cend();) {
        if (it->second->waiting_clients.empty() && it->second->is_full) {
            delete it->second;
            it = cache_map.erase(it);
            ++deleted;
        } else {
            ++it;
        }
    }
    return deleted;
}

void at_exit(poll_requests *p_requests) {
    for (int i = 0; i < p_requests->count; ++i) {
        close(p_requests->requests[i].fd);
    }
}

bool add_to_poll_requests(poll_requests *p_requests, int fd, bool write, bool read) {
    if (p_requests->count == p_requests->size) {
        auto *new_poll_requests = (struct pollfd *) realloc(p_requests->requests,
                                                                     p_requests->size * 2 * sizeof(struct pollfd));
        if (!new_poll_requests) {
            return false;
        }
        p_requests->requests = new_poll_requests;
        p_requests->size *= 2;
    }
    p_requests->requests[p_requests->count].fd = fd;
    short events = 0;
    if (write) {
        events |= POLLOUT;
    }
    if (read) {
        events |= POLLIN;
    }
    p_requests->requests[p_requests->count++].events = events;
    return true;
}

bool add_to_poll_requests_memcheck(poll_requests *p_requests, int fd, bool write, bool read) {
    if (!add_to_poll_requests(p_requests, fd, write, read)) {
        clean_cache_map();
        if (!add_to_poll_requests(p_requests, fd, write, read)) {
            return false;
        }
    }
    return true;
}

struct pollfd *get_from_poll_requests(int fd, poll_requests *p_requests) {
    for (int i = 0; i < p_requests->count; ++i) {
        if (p_requests->requests[i].fd == fd) {
            return p_requests->requests + i;
        }
    }
    return nullptr;
}

void init_poll_requests(poll_requests *p_requests, int listen_fd) {
    p_requests->size = POLL_REQUESTS_START_SIZE;
    p_requests->count = 0;
    p_requests->listen_fd = listen_fd;
    if (!(p_requests->requests = (struct pollfd *)malloc(p_requests->size * sizeof(struct pollfd)))) {
        perror("pollfd structs allocating memory");
        exit(EXIT_FAILURE);
    }
    add_to_poll_requests(p_requests, listen_fd, false, true);
}

void delete_from_poll_requests(poll_requests *p_requests, int fd) {
    for (int i = 0; i < p_requests->count; ++i) {
        if (p_requests->requests[i].fd == fd) {
            p_requests->requests[i] = p_requests->requests[p_requests->count - 1];
            --p_requests->count;
        }
    }
}

bool buf_map_insert(int fd, http_buf &&buf) {
    try {
        buf_map.insert({fd, buf});
    } catch (const std::bad_alloc &) {
        clean_cache_map();
        try {
            buf_map.insert({fd, buf});
        } catch (const std::bad_alloc &) {
            return false;
        }
    }
    return true;
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

bool int_set_insert(std::set<int> &set, int value) {
    try {
        set.insert(value);
    } catch (std::bad_alloc &) {
        clean_cache_map();
        try {
            set.insert(value);
        } catch (std::bad_alloc &) {
            return false;
        }
    }
    return true;
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
        if ((listen_fd = socket(p->ai_family, p->ai_socktype | SOCK_NONBLOCK, p->ai_protocol)) < 0) {
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
    if (new_client < 0 && errno != EWOULDBLOCK && errno != EAGAIN) {
        perror("accepting new client");
    } else if (new_client >= 0) {
        int flags = fcntl(new_client, F_GETFL);
        if (flags < 0) {
            perror("fcntl");
            return -1;
        }
        if (fcntl(new_client, F_SETFL, flags | O_NONBLOCK) < 0) {
            perror("fcntl");
            return -1;
        }
        printf("accepted new client\n");
    }
    return new_client;
}

void process_listening_poll(poll_requests *p_requests, int idx) {
    struct pollfd *poll = &(p_requests->requests[idx]);
    int new_client = get_new_client(poll->fd);
    if (new_client < 0) {
        return;
    }
    if (!add_to_poll_requests_memcheck(p_requests, new_client, false, true)) {
        close(new_client);
    }
    if (!buf_map_insert(new_client, http_buf(CLIENT_RECV))) {
        delete_from_poll_requests(p_requests, new_client);
        close(new_client);
    }
}

int connect_to_server(const std::string &hostname) {
    printf("Trying to connect to %s\n", hostname.c_str());
    const char *hostname_without_scheme;
    if (hostname.find(http_scheme) != std::string::npos) {
        hostname_without_scheme = hostname.c_str() + strlen(http_scheme);
    } else {
        hostname_without_scheme = hostname.c_str();
    }
    int server_fd;
    struct addrinfo hints{}, *server_info;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int err;
    if ((err = getaddrinfo(hostname_without_scheme, "http", &hints, &server_info)) < 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return -1;
    }
    if ((server_fd = socket(server_info->ai_family, server_info->ai_socktype | SOCK_NONBLOCK, server_info->ai_protocol)) < 0) {
        perror("create socket to connect");
        freeaddrinfo(server_info);
        return -1;
    }
    if (connect(server_fd, server_info->ai_addr, server_info->ai_addrlen) < 0 && errno != EINPROGRESS) {
            freeaddrinfo(server_info);
            perror("connect to server");
            close(server_fd);
            return -1;
    }
    freeaddrinfo(server_info);
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
    return minor_version == 0;
}

bool str_append(std::string &str, const char *buf, size_t bytes_num) {
    try {
        str.append(buf, bytes_num);
    } catch (std::bad_alloc &) {
        clean_cache_map();
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
        clean_cache_map();
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
            clean_cache_map();
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
            clean_cache_map();
            return 0;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        } else {
            perror("send");
        }
    }
    return send_res;
}

int recv_data(int fd, char *buf, size_t buf_len) {
    int recv_res = recv(fd, buf, buf_len, MSG_DONTWAIT);
    if (recv_res < 0) {
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            perror("recv");
        } else {
            return 0;
        }
    }
    return recv_res;
}

void reset_connection(int fd, poll_requests *p_requests) {
    buf_map.erase(fd);
    close(fd);
    delete_from_poll_requests(p_requests, fd);
}

// pointer to cache struct should be perceived as invalid after this function call
void unsub_from_cache(int fd, cache *c) {
    if (c) {
        c->waiting_clients.erase(fd);
        if (c->is_error && c->waiting_clients.empty()) {
            for (auto it = cache_map.begin(); it != cache_map.end(); ++it) {
                if (it->second == c) {
                    cache_map.erase(it);
                    delete c;
                    break;
                }
            }
        }
    }
}

void return_error_to_client(http_buf &buf, struct pollfd *poll, const char *http_error, poll_requests *p_requests) {
    buf.type = CLIENT_SEND;
    poll->events = POLLOUT;
    unsub_from_cache(poll->fd, buf.cache);
    buf.bytes_sent = 0;
    buf.parsed_buf.content_length = strlen(http_error);
    buf.buf.clear();
    if (!str_append(buf.buf, http_error, strlen(http_error))) {
        reset_connection(poll->fd, p_requests);
        return;
    }
    buf.buf.shrink_to_fit();
}

void create_cache_entry(cache **new_cache_entry) {
    try {
        *new_cache_entry = new cache;
    } catch (std::bad_alloc &) {
        clean_cache_map();
        try {
            *new_cache_entry = new cache;
        } catch (std::bad_alloc &) {
            *new_cache_entry = nullptr;
        }
    }
}

void create_server_send(http_buf &buf, struct pollfd *poll, poll_requests *p_requests, bool caching, long request_head_length) {
    if (!extract_hostname_from_request(buf.buf, buf.parsed_buf.path, buf.parsed_buf.method.length())) {
        return_error_to_client(buf, poll, http_bad_request, p_requests);
        return;
    }
    int server_fd = connect_to_server(buf.parsed_buf.path);
    if (server_fd == -1) {
        return_error_to_client(buf, poll, http_server_error, p_requests);
        return;
    }
    if (!buf_map_insert(server_fd, http_buf(SERVER_SEND))) {
        close(server_fd);
        return_error_to_client(buf, poll, http_server_error, p_requests);
        return;
    }
    if (!add_to_poll_requests_memcheck(p_requests, server_fd, true, false)) {
        close(server_fd);
        buf_map.erase(server_fd);
        return_error_to_client(buf, poll, http_server_error, p_requests);
        return;
    }
    http_buf &serv_buf = buf_map.find(server_fd)->second;
    if (caching) {
        create_cache_entry(&serv_buf.cache);
    }
    if (request_head_length != -1) {
        buf.bytes_sent = buf.buf.length();
        buf.parsed_buf.content_length += request_head_length;
        buf.parsed_buf.content_length -= buf.parsed_buf.path.length(); // minus hostname length
        serv_buf.parsed_buf.content_length = buf.parsed_buf.content_length;
    } else {
        buf.parsed_buf.content_length = -1;
        serv_buf.parsed_buf.content_length = -1;
    }
    serv_buf.opposite_fd = poll->fd;
    serv_buf.parsed_buf.method = std::move(buf.parsed_buf.method);
    serv_buf.parsed_buf.path = std::move(buf.parsed_buf.path);
    serv_buf.buf.swap(buf.buf);

    buf.opposite_fd = server_fd;
}

void process_client_receive(http_buf &buf, struct pollfd *poll, poll_requests *p_requests) {
    if (poll->revents & POLLIN) {
        int received = recv_data(poll->fd, transfer_buf, TRANSFER_BUF_SIZE);
        if (received < 0) {
            if (buf.opposite_fd != -1) {
                reset_connection(buf.opposite_fd, p_requests);
            }
            reset_connection(poll->fd, p_requests);
            return;
        }
        if (buf.opposite_fd != -1) { // sending put/post/delete content
            auto server_buf_it = buf_map.find(buf.opposite_fd);
            if (server_buf_it != buf_map.end()) {
                http_buf &server_buf = server_buf_it->second;
                if (!str_append(server_buf.buf, transfer_buf, received)) {
                    reset_connection(buf.opposite_fd, p_requests);
                    return_error_to_client(buf, poll, http_server_error, p_requests);
                    return;
                }
                get_from_poll_requests(buf.opposite_fd, p_requests)->events |= POLLOUT;
                buf.bytes_sent += received;
                if (buf.bytes_sent >= buf.parsed_buf.content_length) {
                    buf.type = CLIENT_SEND;
                    poll->events = 0;
                    buf.bytes_sent = 0;
                    buf.parsed_buf.content_length = -1;
                } else if (received == 0) {
                    reset_connection(buf.opposite_fd, p_requests);
                    return_error_to_client(buf, poll, http_bad_request, p_requests);
                }
            } else {
                return_error_to_client(buf, poll, http_service_unavailable, p_requests);
            }
            return;
        }
        if (!str_append(buf.buf, transfer_buf, received)) {
            return_error_to_client(buf, poll, http_server_error, p_requests);
            return;
        }
        //printf("%s\n", buf.buf.c_str());
        int parse_res = parse_full_http_request(buf);
        if (parse_res == -1 || (parse_res == -2 && received == 0)) {
            return_error_to_client(buf, poll, http_bad_request, p_requests);
        } else if (parse_res == -3) {
            return_error_to_client(buf, poll, http_server_error, p_requests);
        } else if (parse_res >= 0) {
            if (!is_supported_method(buf.parsed_buf.method.c_str())) {
                return_error_to_client(buf, poll, http_method_not_allowed, p_requests);
                return;
            }
            if (!is_version_supported(buf.parsed_buf.version)) {
                return_error_to_client(buf, poll, http_version_unsupported, p_requests);
                return;
            }
            std::string::size_type scheme_in_path = buf.parsed_buf.path.find(http_scheme);
            std::string::size_type scheme_in_request = buf.buf.find(http_scheme);
            if (scheme_in_path != std::string::npos) {
                buf.parsed_buf.path.erase(scheme_in_path, strlen(http_scheme));
            }
            if (scheme_in_request != std::string::npos) {
                buf.buf.erase(scheme_in_request, strlen(http_scheme));
            }
            if (is_get_or_head_method(buf.parsed_buf.method.c_str())) {
                auto found_cache_entry = cache_map.find(buf.buf);
                if (found_cache_entry != cache_map.end()) {
                    buf.cache = found_cache_entry->second;
                    if (!int_set_insert(buf.cache->waiting_clients, poll->fd)) {
                        buf.cache = nullptr;
                        return_error_to_client(buf, poll, http_server_error, p_requests);
                        return;
                    }
                    buf.buf.clear();
                    buf.buf.shrink_to_fit();
                    buf.type = CLIENT_SEND;
                    poll->events = POLLOUT;
                } else {
                    buf.type = CLIENT_SEND;
                    poll->events = 0;
                    create_server_send(buf, poll, p_requests, true, -1);
                }
            } else {
                if (buf.parsed_buf.content_length == -1) {
                    return_error_to_client(buf, poll, http_length_required, p_requests);
                } else { // start sending put/post/delete content
                    create_server_send(buf, poll, p_requests, false, parse_res);
                }
            }
        }
    }
}

void server_delete_cache(cache *c) {
    if (c && !c->is_full) {
        if (!c->waiting_clients.empty()) {
            c->is_error = true;
        } else {
            for (auto it = cache_map.begin(); it != cache_map.end(); ++it) {
                if (it->second == c) {
                    cache_map.erase(it);
                    break;
                }
            }
            delete c;
        }
    }
}

void server_error(http_buf &buf, struct pollfd *poll, const char *http_error, poll_requests *p_requests) {
    int opposite_fd = buf.opposite_fd;
    server_delete_cache(buf.cache);
    reset_connection(poll->fd, p_requests);

    auto client_buf_it = buf_map.find(opposite_fd);
    if (client_buf_it != buf_map.end()) {
        http_buf &client_buf = client_buf_it->second;
        if (!client_buf.cache && client_buf.type == CLIENT_SEND) {
            if (client_buf.bytes_sent == 0) {
                return_error_to_client(client_buf, get_from_poll_requests(opposite_fd, p_requests), http_error, p_requests);
            } else {
                reset_connection(opposite_fd, p_requests);
            }
        }
    }
}

void process_server_send(http_buf &buf, struct pollfd *poll, poll_requests *p_requests) {
    if (poll->revents & POLLERR) {
        server_error(buf, poll, http_service_unavailable, p_requests);
    } else if (poll->revents & POLLOUT) {
        if (buf.parsed_buf.content_length != -1) { // request has a content
            int sent = send_data(poll->fd, buf.buf.c_str(), buf.buf.length());
            if (sent < 0) {
                server_error(buf, poll, http_service_unavailable, p_requests);
                return;
            }
            buf.bytes_sent += sent;
            buf.buf.erase(0, sent);
            if (buf.bytes_sent >= buf.parsed_buf.content_length) {
                buf.buf.shrink_to_fit();
                buf.type = SERVER_RECV;
                poll->events = POLLIN;
            } else if (buf.buf.empty()) {
                poll->events = 0;
            }
        } else { // request doesn't have a content
            int sent = send_data(poll->fd, buf.buf.c_str() + buf.bytes_sent, buf.buf.length() - buf.bytes_sent);
            if (sent < 0) {
                server_error(buf, poll, http_service_unavailable, p_requests);
                return;
            }
            buf.bytes_sent += sent;
            if (buf.bytes_sent >= buf.buf.length()) {
                if (!str_insert(buf.buf, buf.parsed_buf.path, buf.parsed_buf.method.length() + 1)) {
                    server_error(buf, poll, http_server_error, p_requests);
                } else {
                    buf.type = SERVER_RECV;
                    poll->events = POLLIN;
                }
            }
        }
    }
}

void process_server_receive(http_buf &buf, struct pollfd *poll, poll_requests *p_requests) {
    if (poll->revents & POLLIN) {
        int received = recv_data(poll->fd, transfer_buf, TRANSFER_BUF_SIZE);
        if (received < 0) {
            server_error(buf, poll, http_service_unavailable, p_requests);
            return;
        }
        if (buf.cache) { // caching mode, buf.buf contains get or head request
            if (!str_append(buf.cache->response, transfer_buf, received)) {
                server_error(buf, poll, http_server_error, p_requests);
                return;
            }
            auto cache_map_found_record = cache_map.find(buf.buf);
            if (cache_map_found_record != cache_map.end()) { // response has been completely parsed
                if (cache_map_found_record->second != buf.cache) {
                    if (!int_set_insert(cache_map_found_record->second->waiting_clients, buf.opposite_fd)) {
                        server_error(buf, poll, http_server_error, p_requests);
                        return;
                    }
                    auto client_buf_it = buf_map.find(buf.opposite_fd);
                    if (client_buf_it != buf_map.end()) {
                        http_buf &client_buf = client_buf_it->second;
                        client_buf.cache = cache_map_found_record->second;
                        get_from_poll_requests(buf.opposite_fd, p_requests)->events |= POLLOUT;
                    } else {
                        cache_map_found_record->second->waiting_clients.erase(buf.opposite_fd);
                    }
                    server_delete_cache(buf.cache);
                    reset_connection(poll->fd, p_requests);
                    return;
                }
                if (buf.cache->waiting_clients.empty()) {
                    server_delete_cache(buf.cache);
                    reset_connection(poll->fd, p_requests);
                    return;
                } else {
                    for (int waiting_client : buf.cache->waiting_clients) {
                        struct pollfd *client_poll = get_from_poll_requests(waiting_client, p_requests);
                        if (client_poll != nullptr) {
                            client_poll->events |= POLLOUT;
                        } else {
                            buf.cache->waiting_clients.erase(waiting_client);
                        }
                    }
                }
                if (buf.cache->response.length() == buf.parsed_buf.content_length) {
                    buf.cache->is_full = true;
                    reset_connection(poll->fd, p_requests);
                } else if (buf.cache->response.length() > buf.parsed_buf.content_length) {
                    buf.cache->is_error = true;
                    reset_connection(poll->fd, p_requests);
                }
            } else {
                int parse_res = parse_full_http_response(buf.cache->response, buf.parsed_buf);
                if (parse_res == -1 || (parse_res == -2 && received == 0)) {
                    server_error(buf, poll, http_server_error, p_requests);
                } else if (parse_res >= 0) {
                    auto client_buf_it = buf_map.find(buf.opposite_fd);
                    if (client_buf_it != buf_map.end()) {
                        http_buf &client_buf = client_buf_it->second;
                        if (buf.parsed_buf.status == 200 && buf.parsed_buf.content_length != -1 &&
                                                            buf.parsed_buf.content_length <= MAX_CACHED_RESPONSE_SIZE) {
                            if (!cache_map_insert(buf.buf, buf.cache) || !int_set_insert(buf.cache->waiting_clients, buf.opposite_fd)) {
                                server_error(buf, poll, http_server_error, p_requests);
                                return;
                            }
                            client_buf.cache = buf.cache;
                            buf.parsed_buf.content_length += parse_res;
                        } else {
                            client_buf.buf.swap(buf.cache->response);
                            client_buf.parsed_buf.content_length = client_buf.buf.length() + 1;
                            server_delete_cache(buf.cache);
                            buf.cache = nullptr;
                        }
                        get_from_poll_requests(buf.opposite_fd, p_requests)->events |= POLLOUT;
                    } else {
                        server_delete_cache(buf.cache);
                        reset_connection(poll->fd, p_requests);
                    }
                }
            }
        } else { // through mode
            auto client_buf_it = buf_map.find(buf.opposite_fd);
            if (client_buf_it != buf_map.end()) {
                http_buf &client_buf = client_buf_it->second;
                if (!str_append(client_buf.buf, transfer_buf, received)) {
                    server_error(buf, poll, http_server_error, p_requests);
                    return;
                }
                if (client_buf.parsed_buf.content_length == -1) {
                    client_buf.parsed_buf.content_length = received + 1;
                } else {
                    client_buf.parsed_buf.content_length += received;
                }
                get_from_poll_requests(buf.opposite_fd, p_requests)->events |= POLLOUT;
                if (received == 0) {
                    client_buf.parsed_buf.content_length -= 1;
                    reset_connection(poll->fd, p_requests);
                }
            }
            else {
                reset_connection(poll->fd, p_requests);
            }
        }
    }
}

void process_client_send(http_buf &buf, struct pollfd *poll, poll_requests *p_requests) {
    if (poll->revents & POLLERR) {
        unsub_from_cache(poll->fd, buf.cache);
        reset_connection(poll->fd, p_requests);
    } else if (poll->revents & POLLOUT) {
        if (buf.cache) { // cache mode
            if (buf.cache->is_error) {
                unsub_from_cache(poll->fd, buf.cache);
                reset_connection(poll->fd, p_requests);
                return;
            }
            int sent = send_data(poll->fd, buf.cache->response.c_str() + buf.bytes_sent,
                                 buf.cache->response.length() - buf.bytes_sent);
            if (sent < 0) {
                unsub_from_cache(poll->fd, buf.cache);
                reset_connection(poll->fd, p_requests);
                return;
            }
            buf.bytes_sent += sent;
            if (buf.cache->response.length() == buf.bytes_sent) {
                if (!buf.cache->is_full) {
                    poll->events = 0;
                } else {
                    reset_connection(poll->fd, p_requests);
                }
            }
        } else { // through mode
            int sent = send_data(poll->fd, buf.buf.c_str(), buf.buf.length());
            if (sent < 0) {
                reset_connection(poll->fd, p_requests);
                return;
            }
            buf.bytes_sent += sent;
            buf.buf.erase(0, sent);
            if (buf.bytes_sent >= buf.parsed_buf.content_length) {
                reset_connection(poll->fd, p_requests);
            } else if (buf.buf.empty()) {
                poll->events = 0;
            }
        }
    }
}

void process_common_poll(poll_requests *p_requests, int idx) {
    struct pollfd *poll = &(p_requests->requests[idx]);
    int fd = poll->fd;
    auto buf_it = buf_map.find(fd);
    if (buf_it == buf_map.end()) {
        delete_from_poll_requests(p_requests, fd);
        return;
    }
    http_buf &buf = buf_it->second;
    switch(buf.type) {
        case CLIENT_SEND:
            process_client_send(buf, poll, p_requests);
            break;
        case CLIENT_RECV:
            process_client_receive(buf, poll, p_requests);
            break;
        case SERVER_SEND:
            process_server_send(buf, poll, p_requests);
            break;
        case SERVER_RECV:
            process_server_receive(buf, poll, p_requests);
            break;
    }
}

bool process_next_poll(int idx, poll_requests *p_requests) {
    struct pollfd *next_poll = &(p_requests->requests[idx]);
    if (next_poll->fd == p_requests->listen_fd) {
        if (next_poll->revents & POLLIN) {
            process_listening_poll(p_requests, idx);
            return true;
        }
        return false;
    } else {
        if (next_poll->revents & (POLLIN | POLLOUT | POLLERR)) {
            process_common_poll(p_requests, idx);
            return true;
        }
        return false;
    }
}

void process_poll_requests(poll_requests *p_requests, int poll_res) {
    for (int i = 0, k = 0; i < p_requests->count && k < poll_res; ++i) {
        if (process_next_poll(i, p_requests)) {
            ++k;
        }
    }
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
    poll_requests p_requests{};
    init_poll_requests(&p_requests, listen_fd);
    int poll_res;
    printf("Started...\n");
    for (;;) {
        if (is_interrupted) {
            break;
        }
        poll_res = poll(p_requests.requests, p_requests.count, -1);
        if (poll_res < 0) {
            if (errno == EINTR) {
                continue;
            } else if (errno == ENOMEM) {
                if (clean_cache_map()) {
                    continue;
                }
            } else {
                perror("poll");
                at_exit(&p_requests);
                exit(EXIT_FAILURE);
            }
        }
        process_poll_requests(&p_requests, poll_res);
    }
    at_exit(&p_requests);
}
