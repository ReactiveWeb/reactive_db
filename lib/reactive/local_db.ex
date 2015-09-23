defmodule Reactive.LocalDb do
  defstruct localRef: :null

  def open(filename) do
    {:ok,ref}=:eleveldb.open(to_char_list(filename), [create_if_missing: true])
    %Reactive.LocalDb{localRef: ref}
  end

end

defimpl Reactive.Db, for: Reactive.LocalDb do
  require Logger

  def put(_db=%{ localRef: ref },key,val) do
    :eleveldb.put(ref,key,val,[])
  end
  def get(_db=%{ localRef: ref }, key) do
    :eleveldb.get(ref,key,[])
  end
  def delete(_db=%{ localRef: ref}, key) do
    :eleveldb.delete(ref,key,[])
  end

  def scan(_db=%{ localRef: ref}, opts) do
    prefix = Map.get(opts,:prefix,"")
    start = Map.get(opts,:start,:none)
    eend = Map.get(opts,:end, :none)
    limit = Map.get(opts,:limit, 100_000)
    fetchO = Map.get(opts,:fetch, :key_value)
    reverse = Map.get(opts,:reverse, :false)

    {:ok,iterator}=:eleveldb.iterator(ref,[])
    # GO TO START:
    iter_move_result = case start do
      :none -> case prefix do
        "" -> case reverse do
          :true -> :eleveldb.iterator_move(iterator, :last)
          :false -> :eleveldb.iterator_move(iterator, :first)
        end
        _ -> case reverse do
          :true -> raise "reverse scan with prefix need start position"
          :false -> :eleveldb.iterator_move(iterator, prefix)
        end
      end
      _ -> case reverse do
        :true -> :eleveldb.iterator_move(iterator, prefix <> start)
        :false -> :eleveldb.iterator_move(iterator, prefix <> start)
      end
    end

   # Logger.debug("First iterator move result #{inspect iter_move_result}")

    dir = case reverse do
      :true -> :prev
      :false -> :next
    end

    prefixLen=:erlang.byte_size(prefix)
    rresult=do_scan(iterator,iter_move_result,prefix,prefixLen,limit,eend,dir,fetchO,[])
    :lists.reverse(rresult)
  end


  defp fetch(key,value,:key_value) do
    {key,value}
  end
  defp fetch(_key,value,:value) do
    value
  end
  defp fetch(key,_value,:key) do
    key
  end

  defp do_scan(_iterator,_,_,0,_,_,_,acc) do
    acc
  end
  defp do_scan(iterator,iter_move_result,prefix,prefixLen,limit,eend,dir,fetchOpt,acc) do
    case iter_move_result do
      {:ok, bkey, value} ->
        case :erlang.split_binary(bkey,prefixLen) do
          {^prefix,^eend=key} ->
            [fetch(key,value,fetchOpt)|acc]
          {^prefix,key} when dir == :next and key > eend ->
            acc
          {^prefix,key} when dir == :prev and key < eend ->
            acc
          {^prefix,key} ->
            next_iter_move_result=:eleveldb.iterator_move(iterator,dir)
            do_scan(iterator,next_iter_move_result,prefix,prefixLen,limit-1,eend,dir,fetchOpt,[fetch(key,value,fetchOpt)|acc])
          _ -> acc
        end
      _ -> acc
    end
  end


  def delete_scan(_db=%{ localRef: ref}, opts) do
    prefix = Map.get(opts,:prefix, "")
    start = Map.get(opts,:start, :none)
    eend = Map.get(opts,:end, :none)
    limit = Map.get(opts,:limit, 100_000)
    fetchO = Map.get(opts,:fetch, :key_value)
    reverse = Map.get(opts,:reverse, :false)

    {:ok,iterator}=:eleveldb.iterator(ref,[])
    # GO TO START:
    iter_move_result = case start do
      :none -> case prefix do
        "" -> case reverse do
          :true -> :eleveldb.iterator_move(iterator, :last)
          :false -> :eleveldb.iterator_move(iterator, :first)
        end
        _ -> case reverse do
          :true -> raise "reverse scan with prefix need start position"
          :false -> :eleveldb.iterator_move(iterator, prefix)
        end
      end
      _ -> case reverse do
        :true -> :eleveldb.iterator_move(iterator, prefix <> start)
        :false -> :eleveldb.iterator_move(iterator, prefix <> start)
      end
    end

   # Logger.debug("First iterator move result #{inspect iter_move_result}")

    dir = case reverse do
      :true -> :prev
      :false -> :next
    end

    prefixLen=:erlang.byte_size(prefix)
    do_delete_scan(ref,iterator,iter_move_result,prefix,prefixLen,limit,eend,dir,fetchO,0)
  end

  defp do_delete_scan(db,iterator,iter_move_result,prefix,prefixLen,limit,eend,dir,fetchOpt,acc) do
    case iter_move_result do
      {:ok, bkey, value} ->
        case :erlang.split_binary(bkey,prefixLen) do
          {^prefix,^eend=key} ->
            :eleveldb.delete(db,key,[])
            acc+1
          {^prefix,key} ->
            next_iter_move_result=:eleveldb.iterator_move(iterator,dir)
            :eleveldb.delete(db,key,[])
            do_delete_scan(db,iterator,next_iter_move_result,prefix,prefixLen,limit-1,eend,dir,fetchOpt,acc+1)
          _ -> acc
        end
      _ -> acc
    end
  end

end