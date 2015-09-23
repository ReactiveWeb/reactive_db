defprotocol Reactive.Db do
  def put(db,key,val)
  def get(db,key)
  def delete(db,key)
  def scan(db,opts)
  def delete_scan(db,opts)
end