#include "storage/ldbc_loader.hh"

#include <charconv>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <unordered_map>

#include "storage/random_loader.hh"  // shuffled_identity

namespace tuskflow {

namespace {

// String.split("\\|"): trailing empty strings are dropped, which decides
// whether a Comment row has 11 or 12 columns.
void split(std::string_view line, std::vector<std::string_view>& cols) {
  cols.clear();
  size_t start = 0;
  for (;;) {
    size_t bar = line.find('|', start);
    if (bar == std::string_view::npos) {
      cols.push_back(line.substr(start));
      break;
    }
    cols.push_back(line.substr(start, bar - start));
    start = bar + 1;
  }
  while (!cols.empty() && cols.back().empty()) cols.pop_back();
}

int64_t parse_long(std::string_view s) {  // Long.parseLong
  int64_t v = 0;
  auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || p != s.data() + s.size()) {
    std::fprintf(stderr, "cannot parse \"%.*s\" as a long\n",
                 static_cast<int>(s.size()), s.data());
    std::exit(1);
  }
  return v;
}

std::string_view col(const std::vector<std::string_view>& cols, size_t i,
                     std::string_view line) {
  if (i >= cols.size()) {  // ArrayIndexOutOfBoundsException in the original
    std::fprintf(stderr, "missing column %zu: %.*s\n", i,
                 static_cast<int>(line.size()), line.data());
    std::exit(1);
  }
  return cols[i];
}

// getAllCsvFiles: Files.list order (not sorted), regular files ending in
// ".csv".  directory_iterator reads the directory in the same order.
std::vector<std::string> csv_files(const std::string& folder) {
  std::vector<std::string> paths;
  std::error_code ec;
  for (const auto& e : std::filesystem::directory_iterator(folder, ec)) {
    std::string p = std::filesystem::absolute(e.path()).string();
    if (e.is_regular_file() && p.size() >= 4 &&
        p.compare(p.size() - 4, 4, ".csv") == 0)
      paths.push_back(p);
  }
  if (ec) {
    std::fprintf(stderr, "cannot list %s: %s\n", folder.c_str(),
                 ec.message().c_str());
    std::exit(1);
  }
  return paths;
}

class Loader {
public:
  Loader(GraphStorage* storage, std::string path)
      : storage_(storage), path_(std::move(path)) {
    for (int t = 0; t < kLdbcNodeTypes; ++t) {
      g_.range_first[t] = INT32_MAX;
      g_.range_last[t] = INT32_MIN;
    }
  }

  LdbcGraph run() {
    load_nodes();
    load_relationships();
    // populateDatabase: all nodes, then the relationships in id order.
    for (Node* n : nodes_) storage_->unsafe_insert_node(n);
    for (Relationship* r : rels_) storage_->unsafe_insert_relationship(r);
    nodes_.clear();
    rels_.clear();
    g_.number_of_nodes = static_cast<uint32_t>(node_id_);
    g_.number_of_persons = static_cast<uint32_t>(mappings_[kPersonType].size());
    g_.person_mappings = shuffled_identity(g_.number_of_persons);
    if (missing_ > 0)
      std::printf("[DBG] %" PRIu64 " relationship ends not found (mapped to "
                  "node 0, as LongIntHashMap.get does)\n", missing_);
    return std::move(g_);
  }

private:
  GraphStorage* storage_;
  std::string path_;
  LdbcGraph g_;
  std::unordered_map<int64_t, int32_t> mappings_[kLdbcNodeTypes];
  std::vector<Node*> nodes_;
  std::vector<Relationship*> rels_;
  int32_t node_id_ = 0;
  int64_t relationship_id_ = 0;
  uint64_t missing_ = 0;
  std::vector<std::string_view> cols_;

  // LongIntHashMap.get: 0 when the key is absent.
  int32_t mapping(int type, int64_t raw_id) {
    auto it = mappings_[type].find(raw_id);
    if (it != mappings_[type].end()) return it->second;
    ++missing_;
    return 0;
  }

  void add_relationship(int64_t from, int64_t to, int type) {
    if (relationship_id_ >= storage_->rel_capacity()) {
      std::fprintf(stderr, "more relationships than the storage holds "
                           "(--rel_capacity, rounded up to a power of two)\n");
      std::exit(1);
    }
    rels_.push_back(new Relationship(relationship_id_++, from, to, type));
  }

  template <typename F>
  void for_each_row(const std::string& folder, F&& f) {
    std::string line;
    for (const std::string& p : csv_files(folder)) {
      std::ifstream file(p);
      if (!file) {
        std::fprintf(stderr, "input file (%s) does not exist!\n", p.c_str());
        std::exit(1);
      }
      bool header = true;
      while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();  // readLine
        if (header) {  // skip header
          header = false;
          continue;
        }
        split(line, cols_);
        f(std::string_view(line));
      }
    }
  }

  void load_nodes() {
    load_nodes_from_folder("/static/Organisation", kOrganizationType);
    load_nodes_from_folder("/static/Place", kPlaceType);
    load_nodes_from_folder("/static/Tag", kTagType);
    load_nodes_from_folder("/static/TagClass", kTagClassType);
    load_nodes_from_folder("/dynamic/Person", kPersonType);
    load_nodes_from_folder("/dynamic/Post", kPostType);
    load_nodes_from_folder("/dynamic/Comment", kCommentType);
    load_nodes_from_folder("/dynamic/Forum", kForumType);
  }

  void load_nodes_from_folder(const char* folder, int type) {
    int32_t start_node = node_id_;
    for_each_row(path_ + folder, [&](std::string_view line) {
      if (static_cast<uint32_t>(node_id_) >= storage_->node_capacity()) {
        std::fprintf(stderr, "more nodes than the storage holds "
                             "(--node_capacity, rounded up to a power of two)\n");
        std::exit(1);
      }
      nodes_.push_back(get_node(node_id_, line, type));
      ++node_id_;
    });
    g_.range_first[type] = start_node;
    g_.range_last[type] = node_id_ - 1;
  }

  // getNode: the raw id, the label, and the relationships taken from the
  // Comment / Post columns (HAS_CREATOR_RELATIONSHIPS, HAS_REPLY_RELATIONSHIPS).
  // No properties (LOAD_ALL_PROPERTIES = false).
  Node* get_node(int32_t node_id, std::string_view line, int type) {
    static const char* const kLabels[kLdbcNodeTypes] = {
        "Organization", "Place", "Tag", "Tag Class",
        "Comment", "Forum", "Person", "Post"};
    Node* node = new Node(node_id);
    node->add_label(LabelStore::get_mapping(kLabels[type]));
    int64_t id = 0;
    switch (type) {
      case kOrganizationType:
      case kPlaceType:
      case kTagType:
      case kTagClassType:
        id = parse_long(col(cols_, 0, line));
        break;
      case kCommentType: {
        id = parse_long(col(cols_, 3, line));
        std::string_view post = col(cols_, 10, line);
        int64_t post_id = !post.empty() ? parse_long(post) : -1;
        int64_t comment_id =
            cols_.size() == 12 ? parse_long(cols_[11]) : -1;
        add_relationship(node_id,
                         mapping(kPersonType, parse_long(col(cols_, 8, line))),
                         kCommentHasCreatorPerson);
        if (post_id != -1) {
          add_relationship(node_id, mapping(kPostType, post_id),
                           kCommentReplyOfPost);
        } else if (comment_id != -1) {
          add_relationship(node_id, mapping(kCommentType, comment_id),
                           kCommentReplyOfComment);
        }
        ++g_.number_of_comments;
        break;
      }
      case kForumType:
      case kPersonType:
        id = parse_long(col(cols_, 3, line));
        break;
      case kPostType:
        id = parse_long(col(cols_, 3, line));
        add_relationship(node_id,
                         mapping(kPersonType, parse_long(col(cols_, 10, line))),
                         kPostHasCreatorPerson);
        ++g_.number_of_posts;
        break;
    }
    mappings_[type][id] = node_id;  // LongIntHashMap.put (last one wins)
    return node;
  }

  void load_relationships() {
    load_relationships_from_folder("/dynamic/Comment_hasTag_Tag", 5);
    load_relationships_from_folder("/dynamic/Forum_hasTag_Tag", 9);
    load_relationships_from_folder("/dynamic/Forum_hasMember_Person", 11);
    load_relationships_from_folder("/dynamic/Person_workAt_Company", 13);
    load_relationships_from_folder("/dynamic/Person_studyAt_University", 14);
    load_relationships_from_folder("/dynamic/Person_likes_Post", 16);
    load_relationships_from_folder("/dynamic/Person_likes_Comment", 17);
    load_relationships_from_folder("/dynamic/Person_knows_Person",
                                   kPersonKnowsPerson);
    load_relationships_from_folder("/dynamic/Person_hasInterest_Tag", 20);
    load_relationships_from_folder("/dynamic/Post_hasTag_Tag", 23);
  }

  void load_relationships_from_folder(const char* folder, int type) {
    int from_type = 0, to_type = 0;
    switch (type) {  // getRelationship (only the types loaded above)
      case 5: from_type = kCommentType; to_type = kTagType; break;
      case 9: from_type = kForumType; to_type = kTagType; break;
      case 11: from_type = kForumType; to_type = kPersonType; break;
      case 13: from_type = kPersonType; to_type = kOrganizationType; break;
      case 14: from_type = kPersonType; to_type = kOrganizationType; break;
      case 16: from_type = kPersonType; to_type = kPostType; break;
      case 17: from_type = kPersonType; to_type = kCommentType; break;
      case 18: from_type = kPersonType; to_type = kPersonType; break;
      case 20: from_type = kPersonType; to_type = kTagType; break;
      case 23: from_type = kPostType; to_type = kTagType; break;
      default: std::fprintf(stderr, "unexpected type %d\n", type); std::exit(1);
    }
    // Skip creation/deletion dates
    const size_t offset = (type == 11 || (16 <= type && type <= 18)) ? 1 : 0;
    for_each_row(path_ + folder, [&](std::string_view line) {
      int64_t from = parse_long(col(cols_, offset + 2, line));
      int64_t to = parse_long(col(cols_, offset + 3, line));
      add_relationship(mapping(from_type, from), mapping(to_type, to), type);
    });
  }
};

}  // namespace

LdbcGraph load_ldbc_graph(GraphStorage* storage, const std::string& path) {
  LdbcGraph g = Loader(storage, path).run();
  g.person_label = LabelStore::get_mapping("Person");
  g.post_label = LabelStore::get_mapping("Post");
  g.comment_label = LabelStore::get_mapping("Comment");
  return g;
}

}  // namespace tuskflow
