{application, raft_db_a3,
 [{description, "An OTP application"},
  {vsn, "0.2.0"},
  {registered, [a3, a3_sup, a3_state_machine]},
  {mod, {raft_db_app, {local, a3, [a1, a2, a3]}}},
  {applications,
   [kernel,
    stdlib
   ]},
  {env,[]},
  {modules, []},

  {licenses, ["Apache-2.0"]},
  {links, []}
 ]}.
