import { pathToFileURL } from 'node:url';

const entry = process.argv[2];
process.argv.splice(1, 2, entry);
await import(pathToFileURL(entry).href);
// The real module has completed startup, including its shutdown handlers.
console.log('archiver-fixture-booted');
