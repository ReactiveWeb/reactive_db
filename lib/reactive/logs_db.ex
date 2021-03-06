defmodule Reactive.LogsDb do
  require Logger
  
  defmodule LogRef do
    defstruct db: :null, log_id: :null
  end
  
  def create(db,log_id) do
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
      {:ok,value} -> {:ok,:erlang.binary_to_term(value)}
    end
  end
  def delete(logRef=%{ db: db, log_id: log_id},key) do
    Reactive.Db.delete(db,log_id <> ":m:" <> key)
  end

  def scan(logRef=%{ db: db, log_id: log_id},from,to,limit \\ 10_000 ,reverse \\ false) do
    {start,climit} = case from do
                       :begin -> {"a",limit+1}
                       :end -> {"z",limit+1}
                       key -> {"l:" <> key,limit}
                     end
    scr=Reactive.Db.scan(db,%{
      :prefix => log_id <> ":",
      :begin => start,
      :end => case to do
        :begin -> "a"
        :end -> "z"
        key -> "l:" <> key
      end,
      :limit => climit,
      :reverse => reverse
    })
    #Logger.info("scanr=#{inspect scr}")
    lprefix = "l:"
    lprefix_size = byte_size(lprefix)
    lsr=:lists.filtermap(fn({fullKey,value}) ->
      case fullKey do
        << ^lprefix :: binary-size(lprefix_size), key :: binary >> -> {true,{key,:erlang.binary_to_term(value)}}
        _ -> false
      end
    end,scr)
   # Logger.info("log scanr=#{inspect lsr}")
    lsr
  end
end