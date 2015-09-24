defmodule Reactive.LogsDb do
  require Logger
  
  defmodule LogRef do
    defstruct db: :null, log_id: :null
  end
  
  def create(db,log_id) do
    ### TODO: Add begin - end
    Reactive.Db.put(db,log_id <> ":a","log begin")
    Reactive.Db.put(db,log_id <> ":z","log end")

    %LogRef{ db: db, log_id: log_id }
  end
  def remove(logRef=%{ db: db, log_id: log_id}) do
    raise "not implemented"
  end
  def put(logRef=%{ db: db, log_id: log_id},key,value) do
    Reactive.Db.put(db,log_id <> ":l:" <> key,:erlang.term_to_binary(value))
  end
  def get(logRef=%{ db: db, log_id: log_id},key,default \\ :not_found) do
    case Reactive.Db.get(db,log_id <> ":l:" <> key) do
      :not_found -> :not_found
      value -> :erlang.binary_to_term(value)
    end
  end
  def delete(logRef=%{ db: db, log_id: log_id},key) do
    Reactive.Db.delete(db,log_id <> ":m:" <> key)
  end

  def scan(logRef=%{ db: db, log_id: log_id},from,to,limit \\ 10_000 ,reverse \\ false) do
    {start,climit} = case from do
                       :begin -> {log_id <> ":a",limit+1}
                       :end -> {log_id <> ":z",limit+1}
                       key -> {log_id <> ":l:" <> key,limit}
                     end
    scr=Reactive.Db.scan(db,%{
      :prefix => log_id <> ":",
      :begin => start,
      :end => case to do
        :begin -> log_id <> ":a"
        :end -> log_id <> ":z"
        key -> log_id <> ":l:" <> key
      end,
      :limit => climit,
      :reverse => reverse
    })
    lprefix = log_id <> ":m:"
    lprefix_size = byte_size(lprefix)
    :lists.filtermap(fn({fullKey,value}) ->
      case fullKey do
        << ^lprefix :: binary-size(lprefix_size), key >> -> {true,{key,:erlang.binary_to_term(value)}}
        _ -> false
      end
    end,scr)
  end
end