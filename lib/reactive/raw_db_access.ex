defmodule Reactive.RawDbAccess do
  defp get_db(name) do
    case name do
      [] -> Reactive.Entities.get_db()
    end
  end

  def binary_to_json(binary) do
    try do
      term = :erlang.binary_to_term(binary)
      %{ type: "term", data: to_string(:lists.flatten(:io_lib.format("~80tp", [term]))) }
    rescue
      e -> %{ type: "string", data: binary }
    end
  end

  def api_request(:scan,[_ | db_name],_contexts,args) do
    rargs=Map.put(args,:fetch,case Map.get(args,:fetch,"key_value") do
      "key_value" -> :key_value
      "key" -> :key
      "value" -> :value
    end)

    db=get_db(db_name)

    res=Reactive.Db.scan(db,rargs)

    case Map.get(rargs,:fetch,"key_value") do
      :key_value -> Enum.map(res,fn({x,y}) -> %{key: x, value: binary_to_json(y)} end)
      :key -> Enum.map(res,fn(x) -> %{key: x} end)
      :value -> Enum.map(res,fn(x) -> %{value: binary_to_json(x)} end)
    end
  end

  def api_request(:delete_scan,[_ | db_name],_contexts,args) do
    db=get_db(db_name)
    Reactive.Db.delete_scan(db,args)
  end
  
end