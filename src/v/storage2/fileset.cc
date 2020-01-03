#include "storage2/fileset.h"

namespace storage {

template<>
open_fileset::open_fileset(
  file logfile,
  std::vector<file>::iterator auxb,
  std::vector<file>::iterator auxe)
  : _logfile(std::move(logfile)) {
    std::move(auxb, auxe, _ixfiles.begin());
}

open_fileset::open_fileset(open_fileset&& other) noexcept
  : _logfile(std::move(other._logfile))
  , _closed(other._closed) {
    auto& that = other._ixfiles;
    std::move(that.begin(), that.end(), _ixfiles.begin());
    other._closed = true;
}

open_fileset::~open_fileset() noexcept {
    vassert(_closed, "fileset not closed");
}

file& open_fileset::logfile() { return _logfile; }

open_fileset::ix_type_iterator open_fileset::begin() {
    return _ixfiles.begin();
}

open_fileset::ix_type_iterator open_fileset::end() { return _ixfiles.end(); }

open_fileset::ix_type_const_iterator open_fileset::begin() const {
    return _ixfiles.begin();
}

open_fileset::ix_type_const_iterator open_fileset::end() const {
    return _ixfiles.end();
}

future<> open_fileset::flush_logfile() { return _logfile.flush(); }

future<> open_fileset::flush_aux() {
    return seastar::parallel_for_each(
      _ixfiles.begin(), _ixfiles.end(), [](file f) { return f.flush(); });
}

future<> open_fileset::flush_all() {
    return when_all_succeed(flush_logfile(), flush_aux());
}

future<> open_fileset::close() {
    return flush_all()
      .then([this] {
          auto logfile_task = _logfile.close();
          auto auxfiles_task = seastar::parallel_for_each(
            _ixfiles.begin(), _ixfiles.end(), [](file f) { return f.close(); });
          return when_all_succeed(
            std::move(logfile_task), std::move(auxfiles_task));
      })
      .then([this]() mutable { _closed = true; });
}

closed_fileset::closed_fileset(std::filesystem::path logfile)
  : _logfile(std::move(logfile))
  , _ixfiles(segment_indices::files_for(_logfile)) {}

const std::filesystem::path& closed_fileset::logfile() const {
    return _logfile;
}

closed_fileset::ix_const_iterator closed_fileset::begin() const {
    return _ixfiles.begin();
}

closed_fileset::ix_const_iterator closed_fileset::end() const {
    return _ixfiles.end();
}

future<open_fileset> closed_fileset::open() const {
    open_flags of = open_flags::create | open_flags::rw;
    auto logfile_task = open_file_dma(_logfile.string(), of);
    auto mapper = [of](std::filesystem::path f) {
        return open_file_dma(f.string(), of);
    };
    auto reducer = [](std::vector<file>&& v, file f) {
        v.emplace_back(std::move(f));
        return std::move(v);
    };

    auto aux_task = seastar::map_reduce(
      _ixfiles.begin(),
      _ixfiles.end(),
      std::move(mapper),
      std::vector<file>(),
      std::move(reducer));

    return when_all_succeed(std::move(logfile_task), std::move(aux_task))
      .then([](file logfile, std::vector<file> ixfiles) {
          return open_fileset(
            std::move(logfile), ixfiles.begin(), ixfiles.end());
      });
}

bool closed_fileset::operator<(const closed_fileset& other) const {
    return logfile() < other.logfile();
}

} // namespace storage