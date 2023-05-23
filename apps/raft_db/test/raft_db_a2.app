{application, raft_db_a2,
 [{description, "An OTP application"},
  {vsn, "0.2.0"},
  {registered, [a2, a2_sup, a2_state_machine]},
  {mod, {raft_db_app, {local, a2, [a1, a2, a3]}}},
  {applications,
   [kernel,
    stdlib
   ]},
  {env,[]},
  {modules, []},

  {licenses, ["Apache-2.0"]},
  {links, []}
 ]}.
