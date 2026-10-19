import { config } from '../config.js';
import { openDatabase } from '../infra/database.js';

const db = openDatabase(config.databasePath);
db.close();
console.log(`SQLite schema ready at ${config.databasePath}`);
