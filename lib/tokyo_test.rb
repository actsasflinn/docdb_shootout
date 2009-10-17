require 'benchmark'
require 'rubygems'
require 'tokyo_tyrant'
puts "Using ruby-tokyotyrant 0.2.0"

def init_connection
  @db = TokyoTyrant::Table.new('127.0.0.1', 1978)
  @db.clear
  @db.set_index 'age', :decimal
end

def create_lots_of_documents(n=200_000, batch_size=1000)
  count = 0
  (n / batch_size).times do
    docs = {}
    batch_size.times do
      docs["bob#{count}"] = {
        'name' => 'Bob Jones',
        'email' => "bob#{rand(1000)}@example.com",
        'age' => rand(100),
        'birthdate' => Time.at(rand(1_000_000_000)),
        'is_admin?' => rand(2) == 1,
      }
      count += 1
    end
    @db.mput(docs)
  end
end

def perform_queries
  results = @db.query do |q|
    q.add_condition 'age', :numge, '90'
    q.order_by 'birthdate'
    q.limit 1000
  end
  raise ArgumentError, "Unexpected query result: #{results.size}" if results.size != 1000
end

def bulk_delete_documents
  before_count = @db.size
  @db.prepare_query do |q|
    q.add_condition 'age', :numge, '80'
  end.delete
  after_count = @db.size
  raise ArgumentError, "Unexpected delete result: #{before_count} #{after_count}" if before_count == after_count
end

def done
  @db && @db.close
end

begin
  Benchmark.bm(10) do |x|
    x.report('init') { init_connection }
    x.report('create') { create_lots_of_documents }
    x.report('query') { perform_queries }
    x.report('delete') { bulk_delete_documents }
  end
ensure
  done
end