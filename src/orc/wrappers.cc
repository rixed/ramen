// vim: ft=cpp bs=2 ts=2 sts=2 sw=2 expandtab
#include <cassert>
#include <orc/OrcFile.hh>
extern "C" {
#  include <caml/mlvalues.h>
#  include <caml/memory.h>
#  include <caml/alloc.h>
#  include <caml/custom.h>
}

typedef __int128_t int128_t;
typedef __uint128_t uint128_t;

using namespace std;
using namespace orc;

/*
 * Writing ORC files
 */

class LazyWriter {
    string fname;
    unique_ptr<Type> type;
    unsigned const batch_size;
    unsigned const max_batches;
    unsigned num_batches;
  public:
    LazyWriter(string fn, string schema, unsigned bsz, unsigned mb);
    ~LazyWriter();
    void start_write();
    void flush_batch(bool);
    unique_ptr<OutputStream> outStream;
    unique_ptr<Writer> writer;
    unique_ptr<ColumnVectorBatch> batch;
};

LazyWriter::LazyWriter(string fn, string schema, unsigned bsz, unsigned mb) :
  fname(fn), type(Type::buildTypeFromString(schema)),
  batch_size(bsz), max_batches(mb), num_batches(0)
{
}

LazyWriter::~LazyWriter()
{
  flush_batch(false);
};

void LazyWriter::start_write()
{
  outStream = writeLocalFile(fname);
  WriterOptions options;
  options.setRowIndexStride(0); // To disable indexing
  writer = createWriter(*type, outStream.get(), options);
  batch = writer->createRowBatch(batch_size);
  assert(batch);
}

void LazyWriter::flush_batch(bool more_to_come)
{
  if (writer) {
    writer->add(*batch);
    if (!more_to_come || ++num_batches >= max_batches) {
      writer->close();
      writer.reset();
      /* and then we keep using the batch created by the first writer. This is
       * not a problem, as writer->createRowBatch just call the proper
       * createRowBatch for that Type. */
    }
  }
}

#define Handler_val(v) (*((class LazyWriter **)Data_custom_val(v)))

static struct custom_operations handler_ops = {
  "org.happyleptic.ramen.orc.handler",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default,
  custom_compare_ext_default
};

extern "C" value orc_handler_create(value schema_, value batch_sz_, value max_batches_, value path_)
{
  CAMLparam4(schema_, batch_sz_, max_batches_, path_);
  CAMLlocal1(res);
  char const *path = String_val(path_);
  char const *schema = String_val(schema_);
  unsigned batch_sz = Long_val(batch_sz_);
  unsigned max_batches = Long_val(max_batches_);
  LazyWriter *hder = new LazyWriter(path, schema, batch_sz, max_batches);
  res = caml_alloc_custom(&handler_ops, sizeof *hder, 0, 1);
  Handler_val(res) = hder;
  CAMLreturn(res);
}

extern "C" value orc_handler_close(value hder_)
{
  CAMLparam1(hder_);
  LazyWriter *handler = Handler_val(hder_);
  if (handler) {
    Handler_val(hder_) = nullptr;
    delete handler;
  }
  CAMLreturn(Val_unit);
}
