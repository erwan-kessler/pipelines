#include <string>
#include <stdexcept>
#include <ios>
#include <sstream>
#include <optional>
#include <limits>
#include <utility>
#include <vector>
#include <queue>
#include <map>
#include <algorithm>
#include <iostream>

enum class Encoding : uint8_t {
    Ascii = 0,
    Hex = 1,
};

static std::string hex_decode(const std::string& hex) {
    std::string result;
    for (size_t i = 0; i < hex.length(); i += 2) {
        uint8_t byte = 0;
        for (size_t j = 0; j < 2; ++j) {
            char c = hex[i + j];
            if ('0' <= c && c <= '9') {
                byte = byte * 16 + (c - '0');
            } else if ('a' <= c && c <= 'f') {
                byte = byte * 16 + (c - 'a' + 10);
            } else if ('A' <= c && c <= 'F') {
                byte = byte * 16 + (c - 'A' + 10);
            } else {
                throw std::invalid_argument("Invalid hex string");
            }
        }
        result.push_back(byte); // NOLINT
    }
    return result;
}


std::string decode(Encoding encoding, const std::string &msg) {
    switch (encoding) {
        case Encoding::Ascii:
            return msg;
        case Encoding::Hex:
            try {
                return hex_decode(msg);
            } catch (...) {
                throw std::runtime_error("Failed to decode message as hex");
            }
        default:
            throw std::invalid_argument("Invalid encoding value");
    }
}

class Message {
public:
    Message(uint8_t id, Encoding encoding, const std::string &msg)
            : id(id), body(decode(encoding, msg)) {}

    [[nodiscard]] uint8_t get_id() const { return id; }

    [[nodiscard]] const std::string &get_body() const { return body; }

    bool operator<(const Message& other) const {
        return id > other.id; // compare by ID in ascending order
    }

private:
    uint8_t id;
    std::string body;
};

class ParsedMessage {
public:
    ParsedMessage(uint8_t pipeline_id, uint8_t id, Encoding encoding, std::string message, std::optional<uint8_t> next_id)
            : pipeline_id(pipeline_id), id(id), encoding(encoding), message(std::move(message)), next_id(next_id) {}

    static ParsedMessage parse(const std::string &line) {
        auto tokens = split(line, ' ');

        if (tokens.size() < 4) {
            throw std::runtime_error("Missing fields");
        }

        auto pipeline_id = parse_uint8(tokens[0]);
        auto id = parse_uint8(tokens[1]);
        auto encoding = static_cast<Encoding>(parse_uint8(tokens[2]));
        auto message = tokens[3];
        auto next_id = parse_optional_uint8(tokens[4]);

        return {pipeline_id, id, encoding, message, next_id};
    }

    [[nodiscard]] uint8_t get_pipeline_id() const { return this->pipeline_id; }

    [[nodiscard]] uint8_t get_id() const { return this->id; }

    [[nodiscard]] Encoding get_encoding() const { return this->encoding; }

    [[nodiscard]] const std::string &get_message() const { return this->message; }

    [[nodiscard]] std::optional<uint8_t> get_next_id() const { return this->next_id; }

    std::string message;
private:
    static std::vector<std::string> split(const std::string &str, char delimiter) {
        std::vector<std::string> tokens;
        std::string token;
        std::istringstream tokenStream(str);
        while (std::getline(tokenStream, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }

    static uint8_t parse_uint8(const std::string &str) {
        try {
            uint64_t val = std::stoull(str);
            if (val > std::numeric_limits<uint8_t>::max()) {
                throw std::out_of_range("");
            }
            return static_cast<uint8_t>(val);
        } catch (const std::exception &ex) {
            throw std::runtime_error("Invalid uint8_t value: " + str);
        }
    }

    static std::optional<uint8_t> parse_optional_uint8(const std::string &str) {
        if (str == "-1") {
            return std::nullopt;
        } else {
            return parse_uint8(str);
        }
    }

    uint8_t pipeline_id;
    uint8_t id;
    std::optional<uint8_t> next_id;
    Encoding encoding;
};

using PipelineId = uint8_t;

struct Pipeline {
    PipelineId id;
    std::optional<uint8_t> next_id;
    bool closed;
    std::priority_queue<Message> messages;

    explicit Pipeline(PipelineId id) : id(id), closed(false) {}
};

struct PipelinesConfig {
    bool discard_invalid_next_id;
};


class Pipelines {
public:
    explicit Pipelines(PipelinesConfig config = PipelinesConfig()) : config(config) {}

    void insert_message(ParsedMessage msg) {
        auto [it, inserted] = inner.emplace(msg.get_pipeline_id(), Pipeline(msg.get_pipeline_id()));
        auto &pipeline = it->second;

        if (pipeline.closed) {
            std::cerr << "The following message was ignored because the pipeline was closed: " << (unsigned ) msg.get_id() << std::endl;
            return;
        }

        if (pipeline.next_id.has_value() && msg.get_id() != *pipeline.next_id && config.discard_invalid_next_id) {
            std::cerr << "Message " << (unsigned ) msg.get_id() << " was ignored because it's not supposed to be received, should have been id " << (unsigned ) *pipeline.next_id << std::endl;
            return;
        }

        try {
            pipeline.messages.emplace(msg.get_id(), msg.get_encoding(), msg.message.data());
        } catch (const std::exception &e) {
            std::cerr << "Message is not valid " << e.what() << std::endl;
        }

        pipeline.next_id = msg.get_next_id();

        if (!pipeline.next_id.has_value()) {
            // close the pipeline
            pipeline.closed = true;
        }
    }

    void display(std::ostream &os) {
        std::vector<uint8_t> keys;
        for (const auto &[key, _]: inner) {
            keys.push_back(key);
        }
        std::sort(keys.begin(), keys.end());
        for (auto key: keys) {
            auto it = inner.find(key);
            if (it == inner.end()) {
                std::cerr << "Pipelines hashmap was modified in between" << std::endl;
                continue;
            }

            auto &pipeline = it->second;
            os << "Pipeline:" << (unsigned) pipeline.id << std::endl;
            while (!pipeline.messages.empty()) {
                auto msg = pipeline.messages.top();
                os << "\t" << (unsigned)msg.get_id() << "| " << msg.get_body().data() << std::endl;
                pipeline.messages.pop();
            }
        }
    }

    void display(std::ostream &os) const {
        auto pipelines = *this;  // clone the object
        pipelines.display(os);
    }

    friend std::ostream &operator<<(std::ostream &os, const Pipelines &pipelines) {
        pipelines.display(os);
        return os;
    }

private:
    std::map<uint8_t, Pipeline> inner;
    PipelinesConfig config;
};



int main() {
    Pipelines pipelines(PipelinesConfig{});
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) {
            break;
        }
        try{
            auto parsed_msg = ParsedMessage::parse(line);
            pipelines.insert_message(parsed_msg);
        }catch (const std::exception& ex) {
            std::cerr << "Could not parse line `" << line << "` with err: " << ex.what() << std::endl;

        }
    }
    pipelines.display(std::cout);
    return 0;
}
