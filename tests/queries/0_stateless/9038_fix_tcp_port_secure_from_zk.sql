-- Aiven patch 038: getServerPort / tcp_port_secure refactor via tryGetServerPort.
-- The commit routes Context::getServerPort through the new non-throwing
-- tryGetServerPort and changes the unknown-port error from CLUSTER_DOESNT_EXIST
-- to BAD_GET. That error-code transition is the deterministic, SQL-observable
-- facet of this commit, reached through the getServerPort() function.
-- (The headline fix -- getTCPPortSecure reading the bound port from the
-- server_ports map -- only diverges in Aiven's dynamic-port deployment, which a
-- stock test server cannot reproduce; see dossier section 4.)

-- A known, registered port resolves through server_ports (passes pre AND post --
-- getServerPort always read the map; this is a sanity anchor, not the evidence).
SELECT getServerPort('tcp_port') > 0;

-- An unknown port: post-patch throws BAD_GET (170); pre-patch threw
-- CLUSTER_DOESNT_EXIST. This is the evidence-of-causation divergence.
SELECT getServerPort('this_port_does_not_exist'); -- { serverError BAD_GET }
