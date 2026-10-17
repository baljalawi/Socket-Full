"""CSE 434 Group 75 DHT manager (full project).

UDP, one JSON object per datagram. Every request carries a command, the
sender's peer name and a request_id; every response echoes the request_id and
carries status SUCCESS or FAILURE (plus an optional reason).

Supported commands: register, setup-dht, dht-complete, query-dht, leave-dht,
join-dht, dht-rebuilt, deregister, teardown-dht, teardown-complete.
"""

import argparse
import ipaddress
import json
import random
import socket

PORT_MIN, PORT_MAX = 37500, 37999   # Group 75: floor(75/2)*1000 + 500 .. +999


def reply(sock, address, message):
    sock.sendto(json.dumps(message, separators=(",", ":")).encode(), address)


def valid_port(value):
    return isinstance(value, int) and not isinstance(value, bool) and PORT_MIN <= value <= PORT_MAX


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


class Manager:
    def __init__(self, port):
        self.port = port
        self.peers = {}        # name -> {name, ip, m_port, p_port, state}
        self.dht = None        # ring order of peer names; dht[0] is the leader
        self.year = None       # dataset year of the current DHT
        self.waiting = None    # None | "dht-complete" | "dht-rebuilt" | "teardown-complete"
        self.pending = None    # (peer name, "leave" | "join") while waiting for dht-rebuilt
        self.cache = {}        # (source IP, source port, request id) -> response

    def tuple_of(self, name):
        p = self.peers[name]
        return {"name": p["name"], "ip": p["ip"], "p_port": p["p_port"]}

    def leader(self):
        return self.dht[0] if self.dht else None

    def show_state(self):
        ring = " -> ".join(f"{i}:{n}" for i, n in enumerate(self.dht)) if self.dht else "none"
        states = ", ".join(f"{p['name']}={p['state']}" for p in self.peers.values()) or "none"
        print(f"  STATE ring=[{ring}] peers=[{states}]", flush=True)

    # ------------------------------------------------------------------ handlers
    def handle(self, msg, addr):
        command = msg.get("command")
        name = msg.get("name")
        fail = lambda reason: {"status": "FAILURE", "reason": reason}
        sender = self.peers.get(name)
        authorized = sender is not None and addr == (sender["ip"], sender["m_port"])

        # While a DHT is being built, rebuilt or deleted, only the matching
        # completion message is accepted.
        if self.waiting and command != self.waiting:
            return fail(f"waiting for {self.waiting}")

        if command == "register":
            return self.register(msg, addr, name)
        if not authorized:
            return fail("peer not registered (or message not from its m-port)")

        if command == "setup-dht":
            n, year = msg.get("n"), msg.get("year")
            available = [p for p in self.peers.values() if p["state"] == "Free" and p["name"] != name]
            if self.dht is not None:
                return fail("a DHT already exists")
            if sender["state"] != "Free":
                return fail("peer is not Free")
            if not (is_int(n) and n >= 3):
                return fail("n must be at least 3")
            if not (is_int(year) and 1950 <= year <= 2019):
                return fail("year must be 1950..2019")
            if len(available) < n - 1:
                return fail(f"only {len(available) + 1} Free peers registered, need {n}")
            chosen = [sender] + random.sample(available, n - 1)
            sender["state"] = "Leader"
            for p in chosen[1:]:
                p["state"] = "InDHT"
            self.dht = [p["name"] for p in chosen]
            self.year = year
            self.waiting = "dht-complete"
            print(f"  DHT SETUP leader={name} n={n} year={year}; waiting for dht-complete", flush=True)
            return {"status": "SUCCESS", "year": year,
                    "peers": [self.tuple_of(p["name"]) for p in chosen]}

        if command == "dht-complete":
            if name != self.leader():
                return fail("only the leader may send dht-complete")
            self.waiting = None
            print(f"  DHT COMPLETE leader={name}", flush=True)
            return {"status": "SUCCESS"}

        if command == "query-dht":
            if self.dht is None:
                return fail("DHT has not been set up")
            if sender["state"] != "Free":
                return fail("only a Free peer may query the DHT")
            target = random.choice(self.dht)
            print(f"  QUERY by {name}: start at {target}", flush=True)
            return {"status": "SUCCESS", "peer": self.tuple_of(target), "year": self.year}

        if command == "leave-dht":
            if self.dht is None:
                return fail("DHT does not exist")
            if name not in self.dht:
                return fail("peer is not maintaining the DHT")
            self.pending, self.waiting = (name, "leave"), "dht-rebuilt"
            print(f"  LEAVE {name}; waiting for dht-rebuilt", flush=True)
            return {"status": "SUCCESS"}

        if command == "join-dht":
            if self.dht is None:
                return fail("DHT does not exist")
            if sender["state"] != "Free":
                return fail("peer is not Free")
            self.pending, self.waiting = (name, "join"), "dht-rebuilt"
            print(f"  JOIN {name}; contact leader {self.leader()}; waiting for dht-rebuilt", flush=True)
            # The leader's tuple and year are a design extension: the joining
            # peer needs to know whom to contact.
            return {"status": "SUCCESS", "leader": self.tuple_of(self.leader()), "year": self.year}

        if command == "dht-rebuilt":
            new_leader = msg.get("new_leader")
            if self.pending is None or name != self.pending[0]:
                return fail("peer did not initiate the leave/join")
            who, kind = self.pending
            if msg.get("result") == "FAILURE":
                # Design extension: the rebuild failed. If the old DHT was left
                # untouched (join refused before teardown) keep it; otherwise
                # the DHT is gone and every member becomes Free again.
                if kind == "join" and msg.get("intact") is True:
                    print(f"  JOIN of {who} FAILED; DHT unchanged", flush=True)
                else:
                    for member in self.dht + [who]:
                        self.peers[member]["state"] = "Free"
                    self.dht, self.year = None, None
                    print(f"  REBUILD after {kind} of {who} FAILED; DHT deleted, all peers Free", flush=True)
                self.pending, self.waiting = None, None
                return {"status": "SUCCESS"}
            if kind == "leave":
                i = self.dht.index(who)
                ring = self.dht[i + 1:] + self.dht[:i]       # right neighbour first
                self.peers[who]["state"] = "Free"
            else:
                ring = self.dht + [who]                      # joiner appended at id n
            if new_leader not in ring:
                return fail("new-leader is not in the rebuilt DHT")
            k = ring.index(new_leader)
            self.dht = ring[k:] + ring[:k]
            for i, member in enumerate(self.dht):
                self.peers[member]["state"] = "Leader" if i == 0 else "InDHT"
            self.pending, self.waiting = None, None
            print(f"  DHT REBUILT after {kind} of {who}; new leader={new_leader}", flush=True)
            return {"status": "SUCCESS"}

        if command == "deregister":
            if sender["state"] != "Free":
                return fail(f"peer is {sender['state']}, only a Free peer may deregister")
            del self.peers[name]
            print(f"  DEREGISTERED {name}", flush=True)
            return {"status": "SUCCESS"}

        if command == "teardown-dht":
            if self.dht is None or name != self.leader():
                return fail("only the leader of an existing DHT may tear it down")
            self.waiting = "teardown-complete"
            print(f"  TEARDOWN by {name}; waiting for teardown-complete", flush=True)
            return {"status": "SUCCESS"}

        if command == "teardown-complete":
            if name != self.leader():
                return fail("only the leader may send teardown-complete")
            for member in self.dht:
                self.peers[member]["state"] = "Free"
            self.dht, self.year, self.waiting = None, None, None
            print("  TEARDOWN COMPLETE; all DHT peers are Free", flush=True)
            return {"status": "SUCCESS"}

        return fail("unknown command")

    def register(self, msg, addr, name):
        ip, m_port, p_port = msg.get("ip"), msg.get("m_port"), msg.get("p_port")
        try:
            ipaddress.IPv4Address(ip)
            good_ip = True
        except (ValueError, TypeError):
            good_ip = False
        used = {self.port}
        for peer in self.peers.values():
            used.update((peer["m_port"], peer["p_port"]))
        if not (isinstance(name, str) and name.isascii() and name.isalpha() and len(name) <= 15):
            return {"status": "FAILURE", "reason": "name must be 1-15 letters"}
        if name in self.peers:
            return {"status": "FAILURE", "reason": "duplicate peer name"}
        if not (good_ip and addr == (ip, m_port) and valid_port(m_port) and valid_port(p_port)
                and m_port != p_port and m_port not in used and p_port not in used):
            return {"status": "FAILURE", "reason": "invalid address or ports already in use"}
        self.peers[name] = {"name": name, "ip": ip, "m_port": m_port, "p_port": p_port, "state": "Free"}
        print(f"  REGISTERED {name} at {ip} m={m_port} p={p_port}", flush=True)
        return {"status": "SUCCESS"}

    # ------------------------------------------------------------------ main loop
    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(("0.0.0.0", self.port))
            print(f"Manager listening on UDP {self.port}", flush=True)
            while True:
                data, addr = sock.recvfrom(65535)
                try:
                    msg = json.loads(data)
                    if not isinstance(msg, dict) or not isinstance(msg.get("request_id"), str):
                        continue
                    key = (addr[0], addr[1], msg["request_id"])
                    if key in self.cache:              # retransmission: resend same answer
                        reply(sock, addr, self.cache[key])
                        continue
                    command = msg.get("command")
                    print(f"RECV {command} from {msg.get('name')} {addr}", flush=True)
                    result = self.handle(msg, addr)
                    result["request_id"] = msg["request_id"]
                    self.cache[key] = result
                    reply(sock, addr, result)
                    reason = f" ({result['reason']})" if "reason" in result else ""
                    print(f"SEND {result['status']} for {command} to {msg.get('name')}{reason}", flush=True)
                    if result["status"] == "SUCCESS" and command != "query-dht":
                        self.show_state()
                except (UnicodeDecodeError, ValueError, TypeError, KeyError) as exc:
                    print(f"Ignored malformed packet from {addr}: {exc}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("port", type=int, help="manager listening UDP port")
    args = parser.parse_args()
    if not valid_port(args.port):
        parser.error(f"port must be in {PORT_MIN}..{PORT_MAX}")
    try:
        Manager(args.port).run()
    except KeyboardInterrupt:
        print("\nManager terminated", flush=True)


if __name__ == "__main__":
    main()
