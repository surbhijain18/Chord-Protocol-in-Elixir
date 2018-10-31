defmodule Project do

  def run(num_nodes, num_requests) do
    m = trunc :math.log2(num_nodes)
    {:ok, counter} = Counter.start_link(Map.new())
    :global.register_name("COUNTER", counter)
    setup_nodes(num_nodes, m)
    create_finger_tables(num_nodes, m)
    send_requests(num_nodes, num_requests, m)
    results_map = Counter.get_state(counter)
    total_hops = Enum.reduce(Map.values(results_map), 0, fn x, acc -> x + acc end)
    IO.puts "Average hops per request is #{Float.floor(total_hops / Kernel.length(Map.values(results_map)), 3)}"
    :ok
  end

  def send_requests(num_nodes, num_requests, m) do
    Enum.map(1..num_nodes, fn x -> node_requests(Integer.to_string(x),num_requests,m ) end)
  end

  def node_requests(origin, num_requests, m) do
    Enum.map(1..num_requests, fn _ -> origin_id = encode_string(origin, m)
                                key = :crypto.strong_rand_bytes(32) |> Base.encode16()
                                target_id = encode_string(key, m)
                                path(key, origin_id, target_id) end
                              )
  end

  defp setup_nodes(num_nodes, m) do
    Enum.map(1..num_nodes, fn x -> node_id = encode_string(Integer.to_string(x), m)
                            node_identifier = "NODE." <> "0000" <> "#{node_id}"
                            if :global.whereis_name(node_identifier) == :undefined do
                              {:ok, pid} = Node.start_link(%{id: node_id})
                              node_name = node_identifier
                            :global.register_name(node_name, pid)
                            end
                          end )
  end

  defp create_finger_tables(num_nodes, m) do
    Enum.map(1..num_nodes, fn x-> node_id = encode_string(Integer.to_string(x), m)
                                  node_identifier = "NODE." <> "0000" <> "#{node_id}"
                                  node = :global.whereis_name(node_identifier)
                                  if node != :undefined do
                                    Node.generateTable(node, m)
                                  end
                           end)
  end

  defp encode_string(str, m) do
    num = :crypto.hash(:sha, str) |> Base.encode16()
    {int_num, _} = Integer.parse(num, 16)
    rem(int_num, :math.pow(2, m) |> trunc)
  end

  defp path(key, origin_id, target_id) do
    node_identifier  = "NODE." <> "0000" <> "#{origin_id}"
    source = :global.whereis_name(node_identifier)
    Node.findNode(source, {key, target_id})
  end
end

[nodes, requests] = System.argv |> Enum.map(&String.to_integer/1)

Project.run(nodes, requests)
