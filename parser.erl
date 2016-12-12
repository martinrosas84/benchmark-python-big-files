-module(parser).
-export([parser/0]).


parser() ->
	{ok, File} = file:open("prueba.txt", read),
	T1 = calendar:universal_time(),
	do_stuff(File, false),
	T2 = calendar:universal_time(),
	{_, Time} = calendar:time_difference(T1, T2),
	erlang:display(Time),
	file:close(File).
	

do_stuff(File, First) ->
	case file:read_line(File) of
		{ok, _} when First -> do_stuff(File);
		{ok, Algo} -> process(Algo),
				   do_stuff(File);
		eof -> io:format("Termineeee")
	end.
do_stuff(File) ->
	case file:read_line(File) of
		{ok, Algo} -> process(Algo),
					  do_stuff(File);
		eof -> io:format("Termineeee")
	end.

process(Var) ->
	erlang:display(Var).