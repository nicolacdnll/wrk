-- Copyright (c) 2019, Barcelona Supercomputing Center (BSC)

-- Script that allows targeting an arbitrary number of streams.
-- Streams are spread among all connections in a round-robin fashion.
-- Streams are composed of different consecutive segments; similarly to
-- MPEG-DASH MPD format). Both streams and segment ids start from 1

-- Usage:
-- This script require 9 parameters to work. To pass the parameters, append them
-- to wrk using `./wrk --script scripts/query.lua.dc [...] -- param1 param2 ...`
-- The 7 parameter are the following: -- path streams_fstring segment_fstring #streams #threads #connections #delay
--  streams_fstring  Formated stream name with an integer e.g., stream_%d.
--  segment_fstring  Formated segment name with an integer e.g., segment_%d.m4s.
--  segment_first    Number of the first segment.
--  segment_last     Number of the last segment. Once reached this segment,
--                    the scripts will start again from segment_first.
--  #streams         Number of streams to target.
--  #threads         Number of threads, same value passed to WRK.
--  #connections     Number of connections, same value passed to WRK.
--  delay           Delay between consecutive requests of the same connection.
local threadcounter = 1
local threads = {}

-- Path to streams
local _streams_name = "stream_%d"
local _segments_name= "segment_%d.m4s"
local _segments_first = 1
local _segments_last  = 1

-- Number of connection per each thred
local _num_cns_thr = 1
-- Stream ids per each connection
local _stream_ids = {}
-- Segment id per each stream
local _stream_sgmt_ids = {}
-- Connection index each thread to round robin them
local _thread_cns_idx = {}
-- Delay between consecutive requests on the same connection (ms)
local _delay = 1000

function setup(thread)
  thread:set("id", threadcounter)
  table.insert(threads, thread)
  threadcounter = threadcounter + 1
end

function init(args)
  --print ("INIT: 0-> " .. args[0] .. ", 1 -> " .. args[1])
  _streams_name = args[1]
  _segments_name = args[2]
  _segments_first = tonumber(args[3])
  _segments_last  = tonumber(args[4])

  -- Number of different target streams
  local num_target_streams = tonumber(args[5])
  -- Number of threads
  local num_threads = tonumber(args[6])
  -- Number of connections among all tread aka. clients
  local num_cns = tonumber(args[7])
  _num_cns_thr = math.floor(num_cns/num_threads)

  _delay = tonumber(args[8])

  -- Each thread initializes its streams id and segment tables
  for cid=1,_num_cns_thr,1 do
    -- Global index for this connection
    local cn_ndx = (id-1)*_num_cns_thr+cid
    -- Assign a stream to each connection using round robin and starting from 1
    -- print("Connection "..cn_ndx.." -> "..(cn_ndx-1)%num_target_streams+1)
    _stream_ids[cn_ndx] = (cn_ndx-1)%num_target_streams+1

    -- All steams start from segment number 1
    _stream_sgmt_ids[cn_ndx] = _segments_first
  end
  -- Each thread starts from the first connection assigned to it, LUA counts from 1
  _thread_cns_idx[id] = 1
end

function delay()
  -- print("delay: ".._delay)
  return _delay
end

function fdelay()
  local r = math.random(0, 50)
  return r
end

request = function()
  local th_cn_idx = _thread_cns_idx[id]
  -- Round robin the connections of each thread
  if th_cn_idx + 1 <= _num_cns_thr
  then
    _thread_cns_idx[id] = th_cn_idx + 1
  else
    _thread_cns_idx[id] = 1
  end

  -- Connection index in the global table
  local gb_cn_idx = (id-1)*_num_cns_thr+th_cn_idx
  local stream = string.format(_streams_name, _stream_ids[gb_cn_idx])
  local segment = string.format(_segments_name, _stream_sgmt_ids[gb_cn_idx])

  -- Request the following segment or start from the first if no more segments
  if _stream_sgmt_ids[gb_cn_idx] + 1 <= _segments_last
  then
    _stream_sgmt_ids[gb_cn_idx] = _stream_sgmt_ids[gb_cn_idx] + 1
  else
    _stream_sgmt_ids[gb_cn_idx] = _segments_first
  end

  path = "/"..stream.."/"..segment
  path = path.."?thread="..id.."&th_cn_idx="..th_cn_idx.."&bg_cn_idx="..gb_cn_idx
  -- print (os.time() .." - Requesting " .. path)
  return wrk.format("GET", path)
end
