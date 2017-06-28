function foreach(arr, f)
{
    for (var c = 0; c < arr.length; c++) f(arr[c], c);
}

function find(arr, f)
{
    for (var c = 0; c < arr.length; c++) {
        if (f(arr[c])) { return arr[c]; }
    }
}

function map(arr, f)
{
    var r = [];
    foreach(arr, function (e) { r.push(f(e)); });
    return r;
}

function keys(arr)
{
    var keys = []
    for (var k in arr) { keys.push(k); }
    return keys;
}

function fold(arr, i, f)
{
    foreach(arr, function (e) { i = f(e, i); });
    return i;
}

function filter(arr, f)
{
  return fold(arr, [], function (e, lst) {
    if (f(e)) {
      return lst.push(e);
    } else {
      return lst;
    }
  });
}
