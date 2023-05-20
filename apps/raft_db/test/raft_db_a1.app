{application, raft_db_a1,
 [{description, "An OTP application"},
  {vsn, "0.2.0"},
  {registered, []},
  {mod, {raft_db_app, {local, a1, [a1, a2, a3]}}},
  {applications,
   [kernel,
    stdlib
   ]},
  {env,[]},
  {modules, []},

  {licenses, ["Apache-2.0"]},
  {links, []}
 ]}.
