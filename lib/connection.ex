defmodule Kadabra.Connection do
  @moduledoc """
  Worker for maintaining an open HTTP/2 connection.
  """

  defstruct buffer: "",
            client: nil,
            uri: nil,
            scheme: :https,
            opts: [],
            socket: nil,
            stream_id: 1,
            reconnect: true,
            encoder_state: nil,
            decoder_state: nil,
            pp_frame: nil,
            transport: nil,
            streams: %{}

  use GenServer
  require Logger

  alias Kadabra.{Encodable, Error, Frame, Http2}
  alias Kadabra.SyncStream, as: Stream
  alias Kadabra.Frame.{Continuation, Data, Goaway, Headers, Ping,
    PushPromise, RstStream, WindowUpdate}

  @data 0x0
  @headers 0x1
  @rst_stream 0x3
  @settings 0x4
  @push_promise 0x5
  @ping 0x6
  @goaway 0x7
  @window_update 0x8
  @continuation 0x9

  def start_link(uri, pid, opts \\ []) do
    GenServer.start_link(__MODULE__, {:ok, uri, pid, opts})
  end

  def init({:ok, uri, pid, opts}) do
    case do_connect(uri, opts) do
      {:ok, socket} ->
        state = initial_state(socket, uri, pid, opts)
        {:ok, state}
      {:error, error} ->
        {:stop, error}
    end
  end

  defp initial_state(socket, uri, pid, opts) do
   encoder = :hpack.new_context
   decoder = :hpack.new_context
   %__MODULE__{
      client: pid,
      uri: uri,
      scheme: opts[:scheme] || :https,
      opts: opts,
      socket: socket,
      reconnect: opts[:reconnect],
      encoder_state: encoder,
      decoder_state: decoder,
      transport: transport(opts[:scheme])
    }
  end

  def do_connect(uri, opts) do
    case opts[:scheme] do
      :http ->
        case :gen_tcp.connect(uri, opts[:port], tcp_options(opts[:tcp])) do
          {:ok, socket} ->
            :gen_tcp.send(socket,  Http2.connection_preface)
            :gen_tcp.send(socket,  Http2.settings_frame)
            {:ok, socket}
          {:error, reason} ->
            {:error,reason}
        end
      :https ->
        :ssl.start()
        case :ssl.connect(uri, opts[:port], ssl_options(opts[:ssl])) do
          {:ok, ssl} ->
            :ssl.send(ssl, Http2.connection_preface)
            :ssl.send(ssl, Http2.settings_frame)
            {:ok, ssl}
          {:error, reason} ->
            {:error, reason}
        end
      _ -> {:error, :bad_scheme}
    end
  end
  defp tcp_options(nil), do: tcp_options([])
  defp tcp_options(opts) do
    opts ++ [
      {:active, :once},
      {:packet, :raw},
      {:reuseaddr, false},
      :binary
    ]
  end
  defp ssl_options(nil), do: ssl_options([])
  defp ssl_options(opts) do
    opts ++ [
      {:active, :once},
      {:packet, :raw},
      {:reuseaddr, false},
      {:alpn_advertised_protocols, [<<"h2">>]},
      :binary
    ]
  end

  def handle_call(:get_info, _from, state) do
    {:reply, {:ok, state}, state}
  end

  def handle_cast({:send, :headers, sender, headers}, state) do
    new_state = do_send_headers(sender, headers, nil, state)
    {:noreply, inc_stream_id(new_state)}
  end

  def handle_cast({:send, :headers, sender, headers, payload}, state) do
    new_state = do_send_headers(sender, headers, payload, state)
    {:noreply, inc_stream_id(new_state)}
  end

  def handle_cast({:send, :goaway}, state) do
    do_send_goaway(state)
    {:noreply, inc_stream_id(state)}
  end


  def handle_cast({:recv, %Frame.Goaway{} = frame}, state) do
    do_recv_goaway(frame, state)
    {:noreply, state}
  end

  def handle_cast({:recv, %Frame.Settings{} = frame}, state) do
    state = do_recv_settings(frame, state)
    {:noreply, state}
  end

  def handle_cast({:recv, %Frame.Ping{ack: ack}}, %{client: pid} = state) do
    resp = if ack, do: :pong, else: :ping
    send(pid, {resp, self()})
    {:noreply, state}
  end

  def handle_cast({:recv, %Frame.WindowUpdate{}}, state) do
    # TODO: Handle window updates
    {:noreply, state}
  end

  def handle_cast({:send, :ping}, %{socket: socket, transport: transport} = state) do
    bin = Ping.new |> Encodable.to_bin
    transport.send(socket, bin)
    {:noreply, state}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  defp inc_stream_id(%{stream_id: stream_id} = state) do
    %{state | stream_id: stream_id + 2}
  end

  defp do_send_headers(sender, headers, payload, %{stream_id: id} = state) do
    stream = Stream.new(state, sender, id)
    headers
    |> Stream.send_headers(payload, stream, state)
    |> handle_stream_response
  end

  defp do_send_goaway(%{socket: socket, stream_id: stream_id, transport: transport}) do
    bin = stream_id |> Goaway.new |> Encodable.to_bin
    transport.send(socket, bin)
  end

  defp do_recv_goaway(%Goaway{last_stream_id: id,
                              error_code: error,
                              debug_data: debug}, %{client: pid} = state) do
    log_goaway(error, id, debug)
    send pid, {:closed, self()}
    {:noreply, state}
  end

  def log_goaway(code, id, bin) do
    error = Error.string(code)
    Logger.error "Got GOAWAY, #{error}, Last Stream: #{id}, Rest: #{bin}"
  end

  defp do_recv_settings(%Frame.Settings{settings: settings} = frame,
                        %{socket: socket,
                          client: pid,
                          decoder_state: decoder,
                          transport: transport}  = state) do

    if frame.ack do
      send(pid, {:ok, self()})
      state
    else
      #new_decoder = :hpack.new_max_table_size(settings.max_header_list_size, decoder)
      decoder = :hpack.new_max_table_size(settings.max_header_list_size, decoder)

      settings_ack = Http2.build_frame(@settings, 0x1, 0x0, <<>>)
      transport.send(socket, settings_ack)

      send(pid, {:ok, self()})

      #%{state | decoder_state: new_decoder}
      %{state | decoder_state: decoder}
    end
  end


  def handle_info({:tcp_closed, _socket}, state) do
    maybe_reconnect(state)
  end

  def handle_info({:tcp, _socket, bin}, state) do
    do_recv_tcp(bin, state)
  end
  def handle_info({:ssl, _socket, bin}, state) do
    do_recv_ssl(bin, state)
  end

  def handle_info({:ssl_closed, _socket}, state) do
    maybe_reconnect(state)
  end

  defp do_recv_ssl(bin, %{socket: socket} = state) do
    bin = state.buffer <> bin
    case parse_ssl(socket, bin, state) do
      {:error, bin, state} ->
        :ssl.setopts(socket, [{:active, :once}])
        {:noreply, %{state | buffer: bin}}
    end
  end
  defp do_recv_tcp(bin, %{socket: socket} = state) do
    bin = state.buffer <> bin
    case parse_ssl(socket, bin, state) do
      {:error, bin, state} ->
        :inet.setopts(socket, [{:active, :once}])
        {:noreply, %{state | buffer: bin}}
    end
  end

  def parse_ssl(socket, bin, state) do
    case Kadabra.Frame.new(bin) do
      {:ok, frame, rest} ->
        state = handle_response(frame, state)
        parse_ssl(socket, rest, state)
      {:error, bin} ->
        {:error, bin, state}
    end
  end

  def handle_response(frame, state) when is_binary(frame) do
    Logger.info "Got binary: #{inspect(frame)}"
    state
  end
  def handle_response(frame, state) do
    stream = state.streams[frame.stream_id]
    handle_frame(frame, stream, state)
  end
  defp handle_frame(frame = %{type: @push_promise}, _, state) do
    open_promise_stream(frame, state)
  end
  defp handle_frame(frame = %{type: @ping}, _, state) do
    GenServer.cast(self(), {:recv, Ping.new(frame)})
    state
  end
  defp handle_frame(frame = %{type: @goaway}, _, state) do
    GenServer.cast(self(), {:recv, Goaway.new(frame)})
    state
  end
  defp handle_frame(frame = %{type: @settings}, _, state) do
    handle_settings(frame)
    state
  end
  defp handle_frame(frame = %{type: @window_update}, _, state) do
    GenServer.cast(self(), {:recv, WindowUpdate.new(frame)})
    state
  end
  defp handle_frame(frame, nil, state) do
    Logger.debug("no stream: #{frame.stream_id}")
    state
  end
  defp handle_frame(frame = %{type: @data},  stream, state) do
    Stream.receive(Data.new(frame), stream, state)
    |> handle_stream_response
  end
  defp handle_frame(frame = %{type: @headers},  stream, state) do
    Stream.receive(Headers.new(frame), stream, state)
    |> handle_stream_response
  end
  defp handle_frame(frame = %{type: @rst_stream}, stream, state) do
    Stream.receive(RstStream.new(frame), stream, state)
    |> handle_stream_response
  end
  defp handle_frame(%{type: @continuation}, stream, state) do
    Stream.receive(Continuation.new(stream), stream, state)
    |> handle_stream_response
  end
  defp handle_frame(frame, _, state) do
    Logger.debug("Unknown frame: #{inspect(frame)}")
    state
  end

  defp handle_stream_response({stream, state, nil}) do
    update_stream(state, stream)
  end
  defp handle_stream_response({stream, state, response}) do
    send(stream.client, response)
    update_stream(state, stream)
  end

  def handle_settings(frame) do
    case Frame.Settings.new(frame) do
      {:ok, s} ->
        GenServer.cast(self(), {:recv, s})
      _else ->
        # TODO: handle bad settings
        :error
    end
  end

  defp open_promise_stream(frame, state) do
    pp_frame = PushPromise.new(frame)
    state = %{state | pp_frame: pp_frame }
    stream = Stream.new(state, state.client, pp_frame.stream_id)
    pp_frame
    |> Stream.receive(stream, state)
    |> handle_stream_response
  end

  defp maybe_reconnect(%{reconnect: false, client: pid} = state) do
    Logger.debug "Socket closed, not reopening, informing client"
    send(pid, {:closed, self()})
    {:stop, :normal, state}
  end

  defp maybe_reconnect(%{reconnect: true, uri: uri, opts: opts, client: pid} = state) do
    case do_connect(uri, opts) do
      {:ok, socket} ->
        Logger.debug "Socket closed, reopened automatically"
        {:noreply, reset_state(state, socket)}
      {:error, error} ->
        Logger.error "Socket closed, reopening failed with #{error}"
        send(pid, :closed)
        {:stop, :normal, state}
    end
  end
  defp update_stream(state, stream = %{state: :closed}) do
    %{state | streams: Map.delete(state.streams, stream.id)}
  end
  defp update_stream(state, stream) do
    %{state | streams: Map.put(state.streams, stream.id, stream)}
  end
  defp reset_state(state, socket) do
    enc = :hpack.new_context
    dec = :hpack.new_context
    %{state | encoder_state: enc, decoder_state: dec, socket: socket}
  end

  defp transport(:http), do: :gen_tcp
  defp transport(:https), do: :ssl
end
