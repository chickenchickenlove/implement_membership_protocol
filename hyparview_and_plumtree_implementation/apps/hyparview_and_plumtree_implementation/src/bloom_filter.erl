-module(bloom_filter).

%% API
-export([new/0, add/2, member/2]).

% This module is for 1024 targets and allows 1% false positive.
% The Magnitude of Bit array.
-define(M, 10000).

% The number of hash function.
-define(K, 7).

new() ->
  <<0:10000>>.

add(RoundId, BloomFilter) ->
  lists:foldl(
    fun(Seed, Acc) ->
      Pos = hash(RoundId, Seed),
      set_bit(Pos, Acc)
    end, BloomFilter, seeds()).

member(RoundId, BloomFilter) ->
  lists:all(
    fun(Seed) ->
      Pos = hash(RoundId, Seed),
      is_bit_set(Pos, BloomFilter)
    end, seeds()).

seeds() ->
  [12345, 54321, 11111, 22222, 33333, 44444, 55555].

hash(RoundId, Seed) ->
  HashBin = crypto:hash(sha256, <<Seed:32, RoundId/binary>>),
  <<Int:256/integer>> = HashBin,
  Int rem ?M.

set_bit(Pos, BloomFilter) ->
  ByteIndex = Pos div 8,
  BitIndex = Pos rem 8,
  <<Prefix:ByteIndex/binary, Byte, Suffix/binary>> = BloomFilter,
  NewByte = Byte bor (1 bsl (7 - BitIndex)),
  <<Prefix/binary, NewByte, Suffix/binary>>.

is_bit_set(Pos, Filter) ->
  ByteIndex = Pos div 8,
  BitIndex = Pos rem 8,
  <<_:ByteIndex/binary, Byte, _/binary>> = Filter,
  (Byte band (1 bsl (7 - BitIndex))) =/= 0.