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
    start = Map.get(opts,:begin,:none)
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
          :true ->
            r1=:eleveldb.iterator_move(iterator, prefix <>
              << 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 >> )
            if r1 == {:error, :invalid_iterator} do
              :eleveldb.iterator_move(iterator, :last)
            else
              :eleveldb.iterator_move(iterator, :prev)
            end
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
    :eleveldb.iterator_close(iterator)
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
      {:ok, bkey, value} when byte_size(bkey) >= prefixLen ->
        case :erlang.split_binary(bkey,prefixLen) do
          {^prefix,^eend=key} ->
            [fetch(key,value,fetchOpt)|acc]
          {^prefix,key} when eend != :none and dir == :next and key > eend ->
            acc
          {^prefix,key} when eend != :none and dir == :prev and key < eend ->
            acc
          {^prefix,key} when limit>0 ->
            next_iter_move_result=:eleveldb.iterator_move(iterator,dir)
            nlimit=limit-1
            do_scan(iterator,next_iter_move_result,prefix,prefixLen,nlimit,eend,dir,fetchOpt,[fetch(key,value,fetchOpt)|acc])
          _ -> acc
        end
      _ -> acc
    end
  end


  def delete_scan(_db=%{ localRef: ref}, opts) do
    prefix = Map.get(opts,:prefix, "")
    start = Map.get(opts,:begin, :none)
    eend = Map.get(opts,:end, :none)
    limit = Map.get(opts,:limit, 100_000)
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
          :true ->
            r1=:eleveldb.iterator_move(iterator, prefix <>
              << 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 >> )
            if r1 == {:error, :invalid_iterator} do
              :eleveldb.iterator_move(iterator, :last)
            else
              :eleveldb.iterator_move(iterator, :prev)
            end
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
    count=do_delete_scan(ref,iterator,iter_move_result,prefix,prefixLen,limit,eend,dir,0)
    :eleveldb.iterator_close(iterator)
    count
  end

  defp do_delete_scan(db,iterator,iter_move_result,prefix,prefixLen,limit,eend,dir,acc) do
    case iter_move_result do
      {:ok, bkey, value} when byte_size(bkey) >= prefixLen ->
        case :erlang.split_binary(bkey,prefixLen) do
          {^prefix,^eend=key} ->
           # IO.inspect({:delete,bkey})
            :eleveldb.delete(db,bkey,[])
            acc+1
          {^prefix,key} when eend != :none and dir == :next and key > eend ->
            acc
          {^prefix,key} when eend != :none and dir == :prev and key < eend ->
            acc
          {^prefix,key} ->
            next_iter_move_result=:eleveldb.iterator_move(iterator,dir)
            #IO.inspect({:delete,bkey})
            :eleveldb.delete(db,bkey,[])
            do_delete_scan(db,iterator,next_iter_move_result,prefix,prefixLen,limit-1,eend,dir,acc+1)
          _ -> acc
        end
      _ -> acc
    end
  end

end