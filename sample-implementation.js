const crypto = require("crypto");

class ConsistentHash {
  constructor(virtualNodes = 150) {
    this.virtualNodes = virtualNodes;
    this.ring = new Map(); // position -> server
    this.servers = new Set();
    this.sortedPositions = []; // sorted array of positions for binary search
  }

  // Hash function using MD5
  hash(key) {
    return parseInt(
      crypto.createHash("md5").update(key).digest("hex").substring(0, 8),
      16
    );
  }

  // Add a server to the ring
  addServer(server) {
    if (this.servers.has(server)) {
      console.log(`Server ${server} already exists`);
      return;
    }

    this.servers.add(server);

    // Add virtual nodes for this server
    for (let i = 0; i < this.virtualNodes; i++) {
      const virtualKey = `${server}:${i}`;
      const position = this.hash(virtualKey);
      this.ring.set(position, server);
    }

    this.updateSortedPositions();
    console.log(
      `Added server ${server} with ${this.virtualNodes} virtual nodes`
    );
  }

  // Remove a server from the ring
  removeServer(server) {
    if (!this.servers.has(server)) {
      console.log(`Server ${server} doesn't exist`);
      return;
    }

    this.servers.delete(server);

    // Remove all virtual nodes for this server
    for (let i = 0; i < this.virtualNodes; i++) {
      const virtualKey = `${server}:${i}`;
      const position = this.hash(virtualKey);
      this.ring.delete(position);
    }

    this.updateSortedPositions();
    console.log(`Removed server ${server}`);
  }

  // Update sorted positions array for efficient lookups
  updateSortedPositions() {
    this.sortedPositions = Array.from(this.ring.keys()).sort((a, b) => a - b);
  }

  // Find which server should handle this key
  getServer(key) {
    if (this.sortedPositions.length === 0) {
      throw new Error("No servers available");
    }

    const position = this.hash(key);

    // Binary search for the first position >= our hash
    let left = 0;
    let right = this.sortedPositions.length - 1;

    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (this.sortedPositions[mid] < position) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    // If we're past the last position, wrap around to the first
    const serverPosition =
      this.sortedPositions[left] >= position
        ? this.sortedPositions[left]
        : this.sortedPositions[0];

    return this.ring.get(serverPosition);
  }

  // Get distribution statistics
  getDistribution() {
    const distribution = {};
    this.servers.forEach((server) => {
      distribution[server] = 0;
    });

    // Test with 10000 sample keys
    for (let i = 0; i < 10000; i++) {
      const key = `key_${i}`;
      const server = this.getServer(key);
      distribution[server]++;
    }

    return distribution;
  }

  // Show ring state (useful for debugging)
  showRing() {
    console.log("\nRing state:");
    this.sortedPositions.forEach((pos) => {
      console.log(`Position ${pos}: ${this.ring.get(pos)}`);
    });
  }
}

// Example usage and testing
function demonstrateConsistentHashing() {
  console.log("=== Consistent Hashing Demo ===\n");

  const hashRing = new ConsistentHash(3); // 3 virtual nodes per server for clearer demo

  // Add initial servers
  console.log("1. Adding initial servers...");
  hashRing.addServer("server1");
  hashRing.addServer("server2");
  hashRing.addServer("server3");

  // Test key distribution
  console.log("\n2. Testing key distribution with 3 servers:");
  const events = [
    "event_1234",
    "event_5678",
    "event_9999",
    "event_4567",
    "event_8888",
  ];

  events.forEach((event) => {
    const server = hashRing.getServer(event);
    const hash = hashRing.hash(event);
    console.log(`${event} (hash: ${hash}) -> ${server}`);
  });

  // Show distribution statistics
  console.log("\n3. Distribution across 10,000 keys:");
  let distribution = hashRing.getDistribution();
  Object.entries(distribution).forEach(([server, count]) => {
    const percentage = ((count / 10000) * 100).toFixed(1);
    console.log(`${server}: ${count} keys (${percentage}%)`);
  });

  // Add a new server and see minimal redistribution
  console.log("\n4. Adding server4...");
  hashRing.addServer("server4");

  console.log("\n5. Same events after adding server4:");
  const moved = [];
  const stayed = [];

  events.forEach((event) => {
    const newServer = hashRing.getServer(event);
    const hash = hashRing.hash(event);
    console.log(`${event} (hash: ${hash}) -> ${newServer}`);

    // Note: In a real implementation, you'd track the old assignments
    // This is just for demonstration
  });

  console.log("\n6. New distribution with 4 servers:");
  distribution = hashRing.getDistribution();
  Object.entries(distribution).forEach(([server, count]) => {
    const percentage = ((count / 10000) * 100).toFixed(1);
    console.log(`${server}: ${count} keys (${percentage}%)`);
  });

  // Remove a server
  console.log("\n7. Removing server2...");
  hashRing.removeServer("server2");

  console.log("\n8. Distribution after removing server2:");
  distribution = hashRing.getDistribution();
  Object.entries(distribution).forEach(([server, count]) => {
    const percentage = ((count / 10000) * 100).toFixed(1);
    console.log(`${server}: ${count} keys (${percentage}%)`);
  });
}

// Demonstrate the redistribution problem with simple modulo
function demonstrateSimpleHashing() {
  console.log("\n=== Simple Hash + Modulo (for comparison) ===\n");

  function simpleHash(key) {
    return parseInt(
      crypto.createHash("md5").update(key).digest("hex").substring(0, 8),
      16
    );
  }

  function getServerSimple(key, numServers) {
    return `server${(simpleHash(key) % numServers) + 1}`;
  }

  const events = [
    "event_1234",
    "event_5678",
    "event_9999",
    "event_4567",
    "event_8888",
  ];

  console.log("With 3 servers:");
  const assignments3 = {};
  events.forEach((event) => {
    const server = getServerSimple(event, 3);
    assignments3[event] = server;
    console.log(`${event} -> ${server}`);
  });

  console.log("\nWith 4 servers:");
  let moved = 0;
  events.forEach((event) => {
    const server = getServerSimple(event, 4);
    if (assignments3[event] !== server) {
      console.log(`${event} -> ${server} (MOVED from ${assignments3[event]})`);
      moved++;
    } else {
      console.log(`${event} -> ${server} (stayed)`);
    }
  });

  console.log(
    `\nResult: ${moved}/${events.length} events moved (${(
      (moved / events.length) *
      100
    ).toFixed(1)}%)`
  );
}

// Run the demonstrations
demonstrateConsistentHashing();
demonstrateSimpleHashing();
