open Batteries

module Base = struct
  type 'a t = { mutex : Mutex.t ; x : 'a }
  let make x =
    { mutex = Mutex.create () ; x }
  let with_lock t f =
    Mutex.lock t.mutex ;
    finally
      (fun () -> Mutex.unlock t.mutex)
      f t.x
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


