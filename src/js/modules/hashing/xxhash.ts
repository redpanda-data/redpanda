import { XXHash64 } from "xxhash";

export function RpcXxhash64(data: Buffer) {
  const seed = 0;
  var hash = new XXHash64(seed);
  hash.update(data);
  return hash.digest().readBigUInt64LE(0);
}
