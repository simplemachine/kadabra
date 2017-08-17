defmodule Kadabra.Frame.Data do
  defstruct [:data, :stream_id, end_stream: false ]

  alias Kadabra.Frame.Flags

  def new(%{payload: data, flags: flags, stream_id: stream_id}) do
    %__MODULE__{
      data: data,
      stream_id: stream_id,
      end_stream: Flags.end_stream?(flags)
    }
  end
end
