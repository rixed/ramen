open Batteries

let with_mutex m f x =
  Mutex.lock m ;
  finally
    (fun () -> Mutex.unlock m)
    f x

module Base = struct
  type 'a t = { mutex : Mutex.t ; x : 'a }
  let make x =
    { mutex = Mutex.create () ; x }
  let with_lock t f =
    with_mutex t.mutex f t.x
end

module Flag = struct
  type t = bool Base.t
  let make (x : bool) = Base.make (ref x)
  let is_set t = Base.with_lock t (!)
  let is_unset = not % is_set
  let set t = Base.with_lock t (fun b -> b := true)
  let clear t = Base.with_lock t (fun b -> b := false)
end

module Counter = struct
  type t = int Base.t
  let make (x : int) = Base.make (ref x)
  let get t = Base.with_lock t (!)
  let set t x = Base.with_lock t (fun r -> r := x)
  let incr t = Base.with_lock t incr
  let decr t = Base.with_lock t decr
end

module Set = struct
  type 'a t = 'a Set.t ref Base.t
  let make () = Base.make (ref Set.empty)
  let is_empty t = Base.with_lock t (fun s -> Set.is_empty !s)
  let cardinal t = Base.with_lock t (fun s -> Set.cardinal !s)
  let iter t f = Base.with_lock t (fun s -> Set.iter f !s)
  let filter t f = Base.with_lock t (fun s -> s := Set.filter f !s)
  let add t x = Base.with_lock t (fun s -> s := Set.add x !s)
end
