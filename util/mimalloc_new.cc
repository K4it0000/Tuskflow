// Routes C++ new/delete to mimalloc.  The JVM
// allocates by bumping a thread-local buffer; per-object malloc/free from
// glibc is a cost the original does not pay.
// CMakeLists.txt builds mimalloc without malloc override (MI_OVERRIDE=OFF),
// so the replacement operators call it explicitly.

#include <cstddef>
#include <new>

#include <mimalloc.h>

void* operator new(std::size_t n) {
  void* p = mi_malloc(n);
  if (p == nullptr) throw std::bad_alloc();
  return p;
}
void* operator new[](std::size_t n) {
  void* p = mi_malloc(n);
  if (p == nullptr) throw std::bad_alloc();
  return p;
}
void* operator new(std::size_t n, std::align_val_t a) {
  void* p = mi_malloc_aligned(n, static_cast<std::size_t>(a));
  if (p == nullptr) throw std::bad_alloc();
  return p;
}
void* operator new[](std::size_t n, std::align_val_t a) {
  void* p = mi_malloc_aligned(n, static_cast<std::size_t>(a));
  if (p == nullptr) throw std::bad_alloc();
  return p;
}
void operator delete(void* p) noexcept { mi_free(p); }
void operator delete[](void* p) noexcept { mi_free(p); }
void operator delete(void* p, std::size_t) noexcept { mi_free(p); }
void operator delete[](void* p, std::size_t) noexcept { mi_free(p); }
void operator delete(void* p, std::align_val_t) noexcept { mi_free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { mi_free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  mi_free(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  mi_free(p);
}
