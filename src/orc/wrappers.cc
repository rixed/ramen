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
  public:
    OrcHandler(string schema, string fn, bool with_index, unsigned bsz, unsigned mb);
    ~OrcHandler();
    void start_write();
    void flush_batch(bool);
    unique_ptr<OutputStream> outStream;
    unique_ptr<Writer> writer;
    unique_ptr<ColumnVectorBatch> batch;
    double start, stop;
};

OrcHandler::OrcHandler(string sch, string fn, bool wi, unsigned bsz, unsigned mb) :
  type(Type::buildTypeFromString(sch)), fname(fn), with_index(wi),
  batch_size(bsz), max_batches(mb), num_batches(0)
{
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
    if (!more_to_come || ++num_batches >= max_batches) {
      writer->close();
      writer.reset();
      batch.reset();
      outStream.reset();
      ramen_archive(fname.c_str(), start, stop);
      /* We could keep using the batch created by the first writer,
       * as writer->createRowBatch just call the proper createRowBatch for
       * that Type. */
    }
  }
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

extern "C" value orc_handler_create(value schema_, value path_, value with_index_, value batch_sz_, value max_batches_)
{
  CAMLparam5(schema_, path_, with_index_, batch_sz_, max_batches_);
  CAMLlocal1(res);
  char const *schema = String_val(schema_);
  char const *path = String_val(path_);
  bool with_index = Bool_val(with_index_);
  unsigned batch_sz = Long_val(batch_sz_);
  unsigned max_batches = Long_val(max_batches_);
  OrcHandler *hder =
    new OrcHandler(schema, path, with_index, batch_sz, max_batches);
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
