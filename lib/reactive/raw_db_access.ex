defmodule Reactive.RawDbAccess do
  defp get_db(name) do
    case name do
      [] -> Reactive.Entities.get_db()
    end
  end

  def api_request(:scan,[_ | db_name],_contexts,args) do
    rargs=Map.put(args,:fetch,case Map.get(args,:fetch,"key_value") do
      "key_value" -> :key_value
      "key" -> :key
      "value" -> :value
    end)

    db=get_db(db_name)

    Reactive.Db.scan(db,rargs)
  end
  
end