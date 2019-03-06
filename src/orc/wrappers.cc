// vim: ft=cpp bs=2 ts=2 sts=2 sw=2 expandtab
#include <cassert>
#include <orc/OrcFile.hh>
extern "C" {
#  include <caml/mlvalues.h>
#  include <caml/memory.h>
#  include <caml/alloc.h>
#  include <caml/custom.h>
#  include "../ringbuf/archive.h"
}

typedef __int128_t int128_t;
typedef __uint128_t uint128_t;

using namespace std;
using namespace orc;

/*
 * Writing ORC files
 */

class OrcHandler {
    unique_ptr<Type> type;
    string fname;
    bool const with_index;
    unsigned const batch_size;
    unsigned const max_batches;
    unsigned num_batches;
    bool archive;
    std::vector<char> strs;
  public:
    OrcHandler(string schema, string fn, bool with_index, unsigned bsz, unsigned mb, bool arc);
    ~OrcHandler();
    void start_write();
    void flush_batch(bool);
    char *keep_string(char const *, size_t len);
    unique_ptr<OutputStream> outStream;
    unique_ptr<Writer> writer;
    unique_ptr<ColumnVectorBatch> batch;
    double start, stop;
};

#define MAX_STRS_SIZE 999999

OrcHandler::OrcHandler(string sch, string fn, bool wi, unsigned bsz, unsigned mb, bool arc) :
  type(Type::buildTypeFromString(sch)), fname(fn), with_index(wi),
  batch_size(bsz), max_batches(mb), num_batches(0), archive(arc)
{
  strs.reserve(MAX_STRS_SIZE);  // FIXME: something better than a vector
}

OrcHandler::~OrcHandler()
{
  flush_batch(false);
};

void OrcHandler::start_write()
{
  outStream = writeLocalFile(fname);
  WriterOptions options;
  options.setRowIndexStride(with_index ? 10000 : 0); // To disable indexing
  writer = createWriter(*type, outStream.get(), options);
  batch = writer->createRowBatch(batch_size);
  assert(batch);
}

void OrcHandler::flush_batch(bool more_to_come)
{
  if (writer) {
    writer->add(*batch);
    batch->clear();
    strs.clear();
    if (!more_to_come || ++num_batches >= max_batches) {
      num_batches = 0;
      writer->close();
      writer.reset();
      batch.reset();
      outStream.reset();
      if (archive) ramen_archive(fname.c_str(), start, stop);
      /* We could keep using the batch created by the first writer,
       * as writer->createRowBatch just call the proper createRowBatch for
       * that Type. */
    }
  }
}

/// strs must not reallocate!
char *OrcHandler::keep_string(char const *s, size_t len)
{
  assert(strs.size() + len <= strs.capacity());
  size_t const pos = strs.size();
  while (*s != '\0') strs.push_back(*s++);
  strs.push_back('\0');
  return &strs[pos];
}

#define Handler_val(v) (*((class OrcHandler **)Data_custom_val(v)))

static struct custom_operations handler_ops = {
  "org.happyleptic.ramen.orc.handler",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default
};

extern "C" value orc_handler_create(value schema_, value path_, value with_index_, value batch_sz_, value max_batches_, value archive_)
{
  CAMLparam5(schema_, path_, with_index_, batch_sz_, max_batches_);
  CAMLxparam1(archive_);
  CAMLlocal1(res);
  char const *schema = String_val(schema_);
  char const *path = String_val(path_);
  bool with_index = Bool_val(with_index_);
  unsigned batch_sz = Long_val(batch_sz_);
  unsigned max_batches = Long_val(max_batches_);
  bool archive = Bool_val(archive_);
  OrcHandler *hder =
    new OrcHandler(schema, path, with_index, batch_sz, max_batches, archive);
  res = caml_alloc_custom(&handler_ops, sizeof *hder, 0, 1);
  Handler_val(res) = hder;
  CAMLreturn(res);
}

extern "C" value orc_handler_close(value hder_)
{
  CAMLparam1(hder_);
  OrcHandler *handler = Handler_val(hder_);
  if (handler) {
    Handler_val(hder_) = nullptr;
    delete handler;
  }
  CAMLreturn(Val_unit);
}
