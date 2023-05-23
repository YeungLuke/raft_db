-module(raft_db_name).
-include("raft_db_name.hrl").

-export([names/1]).

names({dist, Name, Nodes}) ->
    Node = node(),
    #names{server_name=Name,
           server={Name, Node},
           servers=[{Name, N} || N <- Nodes],
           sup_name=list_to_existing_atom(atom_to_list(Name)++"_sup"),
           machine_name=list_to_existing_atom(atom_to_list(Name)++"_state_machine"),
           file_name=atom_to_list(Node) ++ ".db"};
names({local, Name, Servers}) ->
    #names{server_name=Name,
           server=Name,
           servers=Servers,
           sup_name=list_to_existing_atom(atom_to_list(Name)++"_sup"),
           machine_name=list_to_existing_atom(atom_to_list(Name)++"_state_machine"),
           file_name=atom_to_list(Name) ++ ".db"}.
