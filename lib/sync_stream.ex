defmodule Kadabra.SyncStream do
  @moduledoc """
  Struct returned from open connections.
  """
  defstruct [:id, headers: [], body: "", client: nil, state: :idle]

  alias Kadabra.{Connection, Encodable, Http2, Stream}
  alias Kadabra.Frame.{Continuation, Data, Headers, PushPromise, RstStream}

  @data 0x0
  @headers 0x1

  @closed :closed
  @half_closed_local :half_closed_local
  @half_closed_remote :half_closed_remote
  @idle :idle
  @open :open
  # @reserved_local :reserved_local
  @reserved_remote :reserved_remote

  def new(%Connection{}, client, stream_id) do
    %__MODULE__{
      id: stream_id,
      client: client,
      state: :idle
    }
  end


  def receive(%RstStream{}, stream = %__MODULE__{state: state}, connection) when
    state in [@open, @half_closed_local, @half_closed_remote, @closed] do
      {%{stream | state: @closed}, connection, finished(stream)}
  end
  def receive(%Continuation{header_block_fragment: fragment}, stream = %{state: @idle}, connection) do
    {:ok, {headers, new_decoder_state}} = :hpack.decode(fragment, connection.decoder_state)
    on_enter(%{stream | headers: headers ++ stream.headers}, %{connection | decoder_state: new_decoder_state}, nil)
  end
  def receive(%PushPromise{header_block_fragment: fragment}, stream = %{state: @idle}, connection) do
    {:ok, {headers, new_decoder_state}} = :hpack.decode(fragment, connection.decoder_state)
    on_enter(%{stream | state: @reserved_remote, headers: headers ++ stream.headers}, %{connection | decoder_state: new_decoder_state}, {:push_promise, Stream.Response.new(stream)})
  end
  def receive(frame = %Headers{header_block_fragment: fragment}, stream, connection) do
    {:ok, {headers, new_decoder_state}} = :hpack.decode(fragment, connection.decoder_state)
    stream = %__MODULE__{stream | headers: stream.headers ++ headers}
    connection = %{connection | decoder_state: new_decoder_state}
    if frame.end_stream do
      on_enter(%{stream | state: @half_closed_remote}, connection, nil)
    else
      {stream, connection, nil}
    end
  end
  def receive(%Data{end_stream: true, data: data}, stream, connection) do
    stream = %__MODULE__{stream | body: stream.body <> data}
    on_enter(%{stream | state: @half_closed_remote}, connection, nil)
  end
  def receive(%Data{data: data}, stream, connection) do
    stream = %__MODULE__{stream | body: stream.body <> data}
    {stream, connection, nil}
  end
  def send_headers(headers, payload, stream, connection) do
    headers = add_headers(headers, connection)
    {:ok, {encoded, new_encoder_state}} = :hpack.encode(headers, connection.encoder_state)
    headers_payload = :erlang.iolist_to_binary(encoded)
    h = Http2.build_frame(@headers, 0x4, stream.id, headers_payload)
    send_payload = h <>
    if payload do
      Http2.build_frame(@data, 0x1, stream.id, payload)
    else
      ""
    end
    connection.transport.send(connection.socket, send_payload)
    on_enter(%{stream | state: @open},%{connection | encoder_state: new_encoder_state}, nil)
  end

  defp on_enter(stream = %__MODULE__{state: @half_closed_remote}, connection, _reply) do
    bin = stream.id |> RstStream.new |> Encodable.to_bin
    connection.transport.send(connection.socket, bin)
    stream = %{stream | state: @closed}
    {stream, connection, finished(stream)}
  end
  defp on_enter(stream, connection, reply), do: {stream, connection, reply}

  defp finished(stream) do
    {:end_stream, Stream.Response.new(stream)}
  end

  defp add_headers(headers, connection) do
    h =
    [
      {":scheme", Atom.to_string(connection.scheme)},
      {":authority", List.to_string(connection.uri)} | headers
    ]
    # sorting headers to have pseudo headers first.
    Enum.sort(h, fn({a, _b}, {c, _d}) -> a < c end)
  end
end
