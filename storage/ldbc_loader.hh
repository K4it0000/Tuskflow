#pragma once

// LDBC SNB graph of the original (benchmarks/data/LdbcLoader,
// benchmarks/data/metadata/LdbcMetadata, and the LDBC branch of
// BenchmarkUtils.populateDatabase / initializeMappings): reads the datagen
// "raw" CSV directories (composite-merged-fk) and numbers the nodes by type,
// in loading order.

#include <cstdint>
#include <string>
#include <vector>

#include "storage/graph_storage.hh"

namespace tuskflow {

// LdbcMetadata: node types (index into the ranges) and relationship types.
enum LdbcNodeType : int {
  kOrganizationType = 0,
  kPlaceType = 1,
  kTagType = 2,
  kTagClassType = 3,
  kCommentType = 4,
  kForumType = 5,
  kPersonType = 6,
  kPostType = 7,
};
constexpr int kLdbcNodeTypes = 8;

constexpr int kCommentHasCreatorPerson = 4;
constexpr int kCommentReplyOfComment = 7;
constexpr int kCommentReplyOfPost = 8;
constexpr int kPersonKnowsPerson = 18;
constexpr int kPostHasCreatorPerson = 24;

struct LdbcGraph {
  uint32_t number_of_nodes = 0;  // DBConfig.INITIAL_NODES_SIZE
  // ranges[type] = (first, last) node id; (start, start - 1) if empty.
  int32_t range_first[kLdbcNodeTypes];
  int32_t range_last[kLdbcNodeTypes];
  uint64_t number_of_comments = 0;
  uint64_t number_of_posts = 0;
  uint32_t number_of_persons = 0;  // mappings[PERSON_TYPE].size()
  // Label ids (LabelStore) of the labels the transactions check.
  int person_label = -1, post_label = -1, comment_label = -1;
  // DBConfig.personMappingsForZipf (USE_ZIPF_DISTRIBUTION_SHUFFLED).
  std::vector<int32_t> person_mappings;

  // LdbcMetadata.messageIds.  Posts and then Comments are loaded one after
  // the other, so the message ids form one range.
  bool is_message(uint32_t node_id) const {
    return static_cast<int32_t>(node_id) >= range_first[kPostType] &&
           static_cast<int32_t>(node_id) <= range_last[kCommentType];
  }
};

// Loads the graph at `path` (the composite-merged-fk directory) into
// `storage`.  Exits if the graph does not fit the storage's arrays.
LdbcGraph load_ldbc_graph(GraphStorage* storage, const std::string& path);

}  // namespace tuskflow
