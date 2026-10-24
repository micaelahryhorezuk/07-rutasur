const expectedSetName = "rs0";
const expectedMember = "mongo:27017";
const expectedConfig = {
  _id: expectedSetName,
  members: [{ _id: 0, host: expectedMember }],
};

let config;
try {
  config = rs.conf();
} catch (error) {
  if (error.code !== 94 && error.codeName !== "NotYetInitialized") {
    throw error;
  }

  print("Replica set sin configurar; iniciando rs0...");
  const result = rs.initiate(expectedConfig);
  if (result.ok !== 1) {
    throw new Error(`No se pudo iniciar rs0: ${tojson(result)}`);
  }
  config = rs.conf();
}

const member = config.members?.[0];
if (
  config._id !== expectedSetName ||
  config.members?.length !== 1 ||
  member.host !== expectedMember ||
  member.votes === 0 ||
  member.arbiterOnly === true
) {
  throw new Error(
    `Configuración incompatible: se esperaba rs0 con un miembro votante ${expectedMember}; actual=${tojson(config)}`,
  );
}

const deadline = Date.now() + 60000;
while (Date.now() < deadline) {
  try {
    const status = rs.status();
    if (status.myState === 1) {
      print("OK: rs0 está configurado y el miembro mongo:27017 es PRIMARY.");
      quit(0);
    }
    print(`Esperando PRIMARY; estado actual: ${status.members?.[0]?.stateStr ?? "desconocido"}`);
  } catch (error) {
    print(`Esperando estado del replica set: ${error.message}`);
  }
  sleep(1000);
}

throw new Error("Timeout: rs0 no alcanzó PRIMARY en 60 segundos.");