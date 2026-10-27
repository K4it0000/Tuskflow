#pragma once

// Graph entities of the original TuskFlow (database/storage/entities/),
// ported with the same object layout: a node is a separately allocated object
// that points to a property map and a label map, each property is its own
// object, and the storage keeps arrays of pointers.  What the JVM does implicitly (zeroed
// arrays, garbage collection) is done explicitly.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "util/chm.hh"

namespace tuskflow {

inline uint32_t next_power_of_two(uint32_t num) {  // Utils.nextPowerOfTwo
  if (num <= 1) return 2;
  uint32_t hob = 1u << (31 - __builtin_clz(num));
  return hob * 2;
}

enum class PropertyType : uint8_t { INT, LONG, FLOAT, DOUBLE, STRING, NULLV };

// PropertyStore: property name -> id.  The original looks the name up in a
// ConcurrentHashMap<String, Integer> on every addProperty(String, ...).  The
// workload uses two names, registered before the run, so a short read-only
// table with the same string comparison is enough.
class PropertyStore {
public:
  static int get_mapping(const std::string& key) {
    auto& names = table();
    for (size_t i = 0; i < names.size(); ++i)
      if (names[i] == key) return static_cast<int>(i);
    names.push_back(key);  // only reached during single-threaded set-up
    return static_cast<int>(names.size() - 1);
  }

private:
  static std::vector<std::string>& table() {
    static std::vector<std::string> names;
    return names;
  }
};

// LabelStore: label name -> id, ids from 0 in order of first use.  As with
// PropertyStore, all names are registered by the loader before the run.
// Deviation: the original's getLabels() maps each id back to its name and
// the transactions compare strings; here they compare ids.
class LabelStore {
public:
  static int get_mapping(const std::string& label) {
    auto& names = table();
    for (size_t i = 0; i < names.size(); ++i)
      if (names[i] == label) return static_cast<int>(i);
    names.push_back(label);  // only reached during single-threaded set-up
    return static_cast<int>(names.size() - 1);
  }

private:
  static std::vector<std::string>& table() {
    static std::vector<std::string> names;
    return names;
  }
};

// Property: immutable (type, id, value).  The original also boxes the value
// (java.lang.Long); here the value is stored inline.
struct Property {
  PropertyType type;
  int propertyId;
  int64_t value;
};

// PropertyMap: array of Property pointers, grown to max(2 * size, 1).
// A Property object is owned by exactly one map; put() deletes the replaced
// one, and put_all_from() moves the other map's objects (the original shares
// them and lets the garbage collector drop the source map).
class PropertyMap {
public:
  PropertyMap() : properties_(new Property*[0]) {}
  ~PropertyMap() {
    for (int i = 0; i < size_; ++i) delete properties_[i];
    delete[] properties_;
  }
  PropertyMap(const PropertyMap&) = delete;
  PropertyMap& operator=(const PropertyMap&) = delete;

  void put(Property* property) {
    for (int i = 0; i < size_; ++i) {
      if (properties_[i]->propertyId == property->propertyId) {
        delete properties_[i];
        properties_[i] = property;
        return;
      }
    }
    resize_properties();
    properties_[size_++] = property;
  }

  void put_all_from(PropertyMap& other) {
    for (int i = 0; i < other.size_; ++i) {
      put(other.properties_[i]);
      other.properties_[i] = nullptr;
    }
    other.size_ = 0;
  }

  void remove_deleted() {
    for (int i = size_ - 1; i >= 0; --i) {
      if (properties_[i]->type == PropertyType::NULLV) {
        delete properties_[i];
        for (int j = i + 1; j < size_; ++j) properties_[j - 1] = properties_[j];
        --size_;
      }
    }
  }

  Property* const* properties() const { return properties_; }
  int size() const { return size_; }

private:
  Property** properties_;
  int size_ = 0;
  int length_ = 0;

  void resize_properties() {
    if (size_ >= length_) {
      int new_size = std::max(2 * size_, 1);
      auto** np = new Property*[new_size];
      if (length_ > 0) std::memcpy(np, properties_, sizeof(Property*) * length_);
      delete[] properties_;
      properties_ = np;
      length_ = new_size;
    }
  }
};

// LabelMap: parallel int[] / boolean[] arrays (empty for the random graph,
// but allocated as in the original; one label per node for LDBC).
class LabelMap {
public:
  LabelMap() : label_ids_(new int[0]), present_(new bool[0]) {}
  ~LabelMap() {
    delete[] label_ids_;
    delete[] present_;
  }
  LabelMap(const LabelMap&) = delete;
  LabelMap& operator=(const LabelMap&) = delete;

  void put(int label_id, bool is_present) {
    for (int i = 0; i < length_; ++i)  // the original scans the whole array
      if (label_ids_[i] == label_id) return;
    resize();
    label_ids_[size_] = label_id;
    present_[size_] = is_present;
    ++size_;
  }
  void put_all(const LabelMap& other) {
    for (int i = 0; i < other.size_; ++i)
      put(other.label_ids_[i], other.present_[i]);
  }
  void remove_deleted() {
    for (int i = size_ - 1; i >= 0; --i) {
      if (!present_[i]) {
        for (int j = i + 1; j < size_; ++j) {
          label_ids_[j - 1] = label_ids_[j];
          present_[j - 1] = present_[j];
        }
        --size_;
      }
    }
  }
  int size() const { return size_; }
  int id(int i) const { return label_ids_[i]; }
  bool present(int i) const { return present_[i]; }

private:
  int* label_ids_;
  bool* present_;
  int size_ = 0;
  int length_ = 0;

  void resize() {
    if (size_ + 1 >= length_) {
      int new_size = std::max(2 * size_, 1);
      int* nl = new int[new_size]();  // Java arrays are zeroed
      bool* np = new bool[new_size]();
      if (length_ > 0) {
        std::memcpy(nl, label_ids_, sizeof(int) * length_);
        std::memcpy(np, present_, sizeof(bool) * length_);
      }
      delete[] label_ids_;
      delete[] present_;
      label_ids_ = nl;
      present_ = np;
      length_ = new_size;
    }
  }
};

class Node {
public:
  explicit Node(int64_t node_id)
      : node_id_(node_id), properties_(new PropertyMap()),
        labels_(new LabelMap()) {}
  ~Node() {
    delete properties_;
    delete labels_;
  }
  Node(const Node&) = delete;
  Node& operator=(const Node&) = delete;

  void add_property(const std::string& key, int64_t value) {
    add_property(PropertyStore::get_mapping(key), value);
  }
  void add_property(int property_id, int64_t value) {
    properties_->put(new Property{PropertyType::LONG, property_id, value});
  }
  void remove_property(int property_id) {
    properties_->put(new Property{PropertyType::NULLV, property_id, 0});
  }

  void add_label(int label_id) { labels_->put(label_id, true); }
  // getLabels().get(0) as a label id; -1 without labels.
  int first_label() const { return labels_->size() > 0 ? labels_->id(0) : -1; }

  int64_t id() const { return node_id_; }
  const PropertyMap& properties() const { return *properties_; }

  // Node.copy(): new node with fresh Property objects and the same labels.
  // Deviation: under the node's merge lock.  A copy can overlap a merge or an
  // in-place write only through the original's scheduler races; Java then reads a stale value, C++ would read freed memory.
  // `ver` (serializability log): when non-null, *version_read
  // receives the node's version under the same lock as the copy.
  Node* copy(const std::atomic<uint32_t>* ver = nullptr,
             uint32_t* version_read = nullptr) const {
    merge_lock_.lock();
    Node* n = copy_unlocked();
    if (ver != nullptr) *version_read = ver->load(std::memory_order_relaxed);
    merge_lock_.unlock();
    return n;
  }
  Node* copy_unlocked() const {
    Node* n = new Node(node_id_);
    Property* const* props = properties_->properties();
    for (int i = 0; i < properties_->size(); ++i) {
      const Property* p = props[i];
      if (p->type != PropertyType::NULLV)
        n->add_property(p->propertyId, p->value);
      else
        n->remove_property(p->propertyId);
    }
    for (int i = 0; i < labels_->size(); ++i)
      n->labels_->put(labels_->id(i), labels_->present(i));
    return n;
  }

  // addProperty on the stored node (2PL writes in place).  Deviation: under
  // the node's merge lock, which the original does not take; an epoch-mode
  // merge may overlap at the switch between modes.
  // Returns the new version if `ver` (serializability log) is non-null.
  uint32_t add_property_in_place(const std::string& key, int64_t value,
                                 std::atomic<uint32_t>* ver = nullptr) {
    merge_lock_.lock();
    add_property(key, value);
    uint32_t v = ver != nullptr ? ver->fetch_add(1) + 1 : 0;
    merge_lock_.unlock();
    return v;
  }

  // Node.merge(other): labels and properties, per entry.  `other` is a local
  // copy that is discarded afterwards, so its Property objects are moved.
  // Deviation: guarded by a per-node lock.  The original merges without
  // synchronisation; two committers can merge into the same node in one
  // epoch (the mammoth and a short transaction that only touched visited
  // nodes), which is a data race in Java and undefined behaviour in C++.
  // Returns the new version if `ver` (serializability log) is non-null.
  uint32_t merge(Node& other, std::atomic<uint32_t>* ver = nullptr) {
    merge_lock_.lock();
    node_id_ = other.node_id_;
    labels_->put_all(*other.labels_);
    labels_->remove_deleted();
    properties_->put_all_from(*other.properties_);
    properties_->remove_deleted();
    uint32_t v = ver != nullptr ? ver->fetch_add(1) + 1 : 0;
    merge_lock_.unlock();
    return v;
  }

private:
  int64_t node_id_;
  PropertyMap* properties_;
  LabelMap* labels_;
  mutable SpinLock merge_lock_;
};

class Relationship {
public:
  Relationship(int64_t rel_id, int64_t start, int64_t end, int type)
      : rel_id_(rel_id), start_(start), end_(end), type_(type) {}
  ~Relationship() { delete properties_; }
  Relationship(const Relationship&) = delete;
  Relationship& operator=(const Relationship&) = delete;

  int64_t id() const { return rel_id_; }
  int64_t start_node() const { return start_; }
  int64_t end_node() const { return end_; }
  int type() const { return type_; }

private:
  int64_t rel_id_, start_, end_;
  int type_;
  PropertyMap* properties_ = nullptr;  // lazily created; unused here
};

// Eclipse Collections MutableIntList (IntArrayList): growable int array.
using IntList = std::vector<int32_t>;

// AtomicObjectArray<T>: capacity rounded to the next power of two, zeroed
// up front (the JVM zeroes a new array).
template <typename T>
class AtomicObjectArray {
public:
  explicit AtomicObjectArray(uint32_t capacity)
      : capacity_(next_power_of_two(capacity)),
        data_(new std::atomic<T*>[capacity_]) {
    for (uint32_t i = 0; i < capacity_; ++i)
      data_[i].store(nullptr, std::memory_order_relaxed);
  }
  ~AtomicObjectArray() { delete[] data_; }
  AtomicObjectArray(const AtomicObjectArray&) = delete;
  AtomicObjectArray& operator=(const AtomicObjectArray&) = delete;

  void put(uint32_t index, T* value) {
    if (data_[index].load(std::memory_order_acquire) == nullptr)
      count_.fetch_add(1, std::memory_order_relaxed);
    data_[index].store(value, std::memory_order_release);
  }
  T* get(uint32_t index) const {
    if (index >= capacity_) return nullptr;
    return data_[index].load(std::memory_order_acquire);
  }
  bool contains(uint32_t index) const {
    return index < capacity_ &&
           data_[index].load(std::memory_order_acquire) != nullptr;
  }
  uint32_t capacity() const { return capacity_; }
  uint32_t size() const { return count_.load(std::memory_order_relaxed); }

private:
  uint32_t capacity_;
  std::atomic<T*>* data_;
  std::atomic<uint32_t> count_{0};
};

}  // namespace tuskflow
