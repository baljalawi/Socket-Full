"""CSE 434 Group 75 DHT peer (full project).

Each peer owns two UDP sockets:
  * m-port: request/response with the manager (main thread only);
  * p-port: peer-to-peer messages.

Threads:
  * main thread   - reads commands from stdin and drives manager exchanges;
  * receiver      - reads the p-port, ACKs every request immediately (hop-by-hop
                    reliability), filters duplicates, and either hands a message
                    to a waiting thread (ring messages that have come back to
                    their origin, query results, rebuild notices) or queues it
                    for the worker;
  * worker        - processes queued peer commands in arrival order, forwarding
                    them around the ring as required.

Peer-to-peer commands: set-id, store, count, teardown, reset-id, rebuild-dht,
join-request, rebuild-done, find-event, find-event-result.
"""

import argparse
import csv
import ipaddress
import json
import queue
import random
import socket
import sys
import threading
import uuid
from collections import OrderedDict
from pathlib import Path

TIMEOUT = 2          # seconds per attempt for one request/ACK exchange
RETRIES = 4          # attempts before giving up
RING_WAIT = 600      # seconds to wait for a message to travel the whole ring
QUERY_WAIT = 15      # seconds to wait for a find-event result
PORT_MIN, PORT_MAX = 37500, 37999
FIELDS = ["event_id", "state", "year", "month_name", "event_type", "cz_type", "cz_name",
          "injuries_direct", "injuries_indirect", "deaths_direct", "deaths_indirect",
          "damage_property", "damage_crops", "tor_f_scale"]

# Ring messages that end back at the peer that started them.
LOOP_COMMANDS = {"count", "teardown", "reset-id"}
# Messages delivered straight to a waiting thread instead of the worker.
REPLY_COMMANDS = {"find-event-result", "rebuild-done"}


def encode(message):
    return json.dumps(message, separators=(",", ":")).encode()


def exchange(sock, address, payload):
    """Send a request and wait for the response with the same request_id.

    Retransmissions reuse the request_id, so the receiver can recognise and
    re-ACK duplicates without acting on them twice.
    """
    payload = dict(payload, request_id=uuid.uuid4().hex)
    packet = encode(payload)
    for _ in range(RETRIES):
        sock.sendto(packet, address)
        try:
            while True:
                raw, source = sock.recvfrom(65535)
                if source != address:
                    continue
                answer = json.loads(raw)
                if answer.get("request_id") == payload["request_id"]:
                    return answer
        except socket.timeout:
            pass
    raise TimeoutError(f"No response from {address} for {payload['command']}")


def first_prime_above(value):
    candidate = value + 1
    while True:
        for divisor in range(2, int(candidate ** 0.5) + 1):
            if candidate % divisor == 0:
                break
        else:
            return candidate
        candidate += 1


def find_dataset(year):
    here = Path(__file__).resolve().parent
    for path in (here / "data" / f"details-{year}.csv", here / f"details-{year}.csv"):
        if path.exists():
            return path
    raise FileNotFoundError(f"details-{year}.csv not found in {here} or {here / 'data'}")


def load_records(year):
    """Read details-YYYY.csv (header skipped) and return (records, table size s)."""
    with find_dataset(year).open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "event_id" not in reader.fieldnames:
            raise ValueError("Expected a storm CSV with an event_id column")
        records = [{k: r.get(k, "") for k in FIELDS} for r in reader]
    for record in records:
        int(record["event_id"])
    return records, first_prime_above(2 * len(records))


class Peer:
    def __init__(self, manager):
        self.manager = manager
        self.m_sock = None
        self.p_sock = None
        self.name = None
        self.ip = None
        self.p_port = None
        # DHT state
        self.my_id = None
        self.ring = []             # list of {name, ip, p_port}; index = identifier
        self.table_size = None
        self.year = None
        self.table = {}            # hash position -> list of records (collisions kept)
        self.lock = threading.Lock()
        # plumbing
        self.work = queue.Queue()
        self.waiters = {}          # token -> Queue for a thread awaiting a message
        self.waiters_lock = threading.Lock()
        self.seen = OrderedDict()  # (source, request_id) -> ACK already sent
        self.acks = {}             # request_id -> Queue awaiting that ACK on the p-port
        self.acks_lock = threading.Lock()
        self.stored = 0            # per-build statistics, reported in the count
        self.forwarded = 0

    # ------------------------------------------------------------------ helpers
    def me(self):
        return {"name": self.name, "ip": self.ip, "p_port": self.p_port}

    def right(self):
        return self.ring[(self.my_id + 1) % len(self.ring)]

    def local_count(self):
        with self.lock:
            return sum(len(bucket) for bucket in self.table.values())

    def manager_request(self, command, **kwargs):
        print(f"SEND {command} to manager {kwargs if kwargs else ''}", flush=True)
        answer = exchange(self.m_sock, self.manager, {"command": command, "name": self.name, **kwargs})
        reason = f" ({answer['reason']})" if answer.get("reason") else ""
        print(f"RECV {command} response from manager: {answer['status']}{reason}", flush=True)
        return answer

    def send(self, destination, command, quiet=False, **kwargs):
        """Send one peer message from this peer's p-port and wait for its ACK.

        All peer traffic leaves from the registered p-port, so every source
        port stays inside the group's range. The receiver thread matches the
        ACK by request_id and hands it to the waiting thread.
        """
        if not quiet:
            print(f"SEND {command} to {destination['name']}", flush=True)
        request_id = uuid.uuid4().hex
        packet = encode({"command": command, **kwargs, "request_id": request_id})
        box = queue.Queue()
        with self.acks_lock:
            self.acks[request_id] = box
        try:
            for _ in range(RETRIES):
                self.p_sock.sendto(packet, (destination["ip"], destination["p_port"]))
                try:
                    answer = box.get(timeout=TIMEOUT)
                    break
                except queue.Empty:
                    continue
            else:
                raise TimeoutError(f"No ACK from {destination['name']} for {command}")
        finally:
            with self.acks_lock:
                self.acks.pop(request_id, None)
        if answer.get("status") != "SUCCESS":
            raise RuntimeError(f"{command} rejected by {destination['name']}: {answer.get('reason')}")
        return answer

    def new_waiter(self):
        token = uuid.uuid4().hex
        with self.waiters_lock:
            self.waiters[token] = queue.Queue()
        return token

    def wait_for(self, token, timeout):
        try:
            return self.waiters[token].get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("timed out waiting for a peer reply") from None
        finally:
            with self.waiters_lock:
                self.waiters.pop(token, None)

    def deliver(self, message):
        with self.waiters_lock:
            box = self.waiters.get(message.get("token"))
        if box is None:
            print(f"Ignored stale {message['command']}", flush=True)
        else:
            box.put(message)

    # ------------------------------------------------------------------ receiver
    def receive_loop(self):
        while True:
            try:
                raw, source = self.p_sock.recvfrom(65535)
            except OSError:
                return                      # socket closed on exit
            try:
                message = json.loads(raw)
                if "command" not in message:      # an ACK for one of our own sends
                    with self.acks_lock:
                        box = self.acks.get(message.get("request_id"))
                    if box is not None:
                        box.put(message)
                    continue
                key = (source, message["request_id"])
                if key in self.seen:        # retransmission: re-ACK, do nothing else
                    self.p_sock.sendto(self.seen[key], source)
                    continue
                ack = encode({"status": "SUCCESS", "request_id": message["request_id"]})
                self.seen[key] = ack
                if len(self.seen) > 200000:
                    self.seen.popitem(last=False)
                self.p_sock.sendto(ack, source)
                command = message.get("command")
                if command in REPLY_COMMANDS or (
                        command in LOOP_COMMANDS and message.get("origin", {}).get("name") == self.name):
                    self.deliver(message)
                else:
                    self.work.put(message)
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                print(f"Malformed peer message from {source}: {exc}", flush=True)

    # ------------------------------------------------------------------ worker
    def work_loop(self):
        handlers = {"set-id": self.on_set_id, "store": self.on_store, "count": self.on_count,
                    "teardown": self.on_teardown, "reset-id": self.on_reset_id,
                    "rebuild-dht": self.on_rebuild_dht, "join-request": self.on_join_request,
                    "find-event": self.on_find_event}
        while True:
            message = self.work.get()
            handler = handlers.get(message.get("command"))
            try:
                if handler is None:
                    print(f"Unknown peer command {message.get('command')}", flush=True)
                else:
                    handler(message)
            except Exception as exc:        # keep the worker alive whatever happens
                print(f"ERROR handling {message.get('command')}: {exc}", flush=True)

    def on_set_id(self, msg):
        self.my_id, self.ring = msg["id"], msg["ring"]
        self.table_size, self.year = msg["table_size"], msg.get("year")
        with self.lock:
            self.table.clear()
        self.stored = self.forwarded = 0
        print(f"RECV set-id: my ID={self.my_id}, ring size={len(self.ring)}, "
              f"right neighbour={self.right()['name']}", flush=True)

    def on_store(self, msg):
        target, pos = msg["target"], msg["pos"]
        if target == self.my_id:
            with self.lock:
                self.table.setdefault(pos, []).append(msg["record"])
            self.stored += 1
        else:
            self.forwarded += 1
            self.send(self.right(), "store", quiet=True, target=target, pos=pos, record=msg["record"])

    def on_count(self, msg):
        counts = msg["counts"] + [{"id": self.my_id, "name": self.name, "count": self.local_count()}]
        print(f"RECV count: storing {self.local_count()} records "
              f"(this build: stored {self.stored}, forwarded {self.forwarded}); "
              f"pass to {self.right()['name']}", flush=True)
        self.stored = self.forwarded = 0
        self.send(self.right(), "count", origin=msg["origin"], token=msg["token"], counts=counts)

    def on_teardown(self, msg):
        right = self.right()
        with self.lock:
            dropped = sum(len(b) for b in self.table.values())
            self.table.clear()
        print(f"RECV teardown from ring: deleted local hash table ({dropped} records)", flush=True)
        if msg.get("final"):
            self.my_id, self.ring, self.table_size, self.year = None, [], None, None
        self.send(right, "teardown", origin=msg["origin"], token=msg["token"], final=msg.get("final"))

    def on_reset_id(self, msg):
        self.my_id, self.ring = msg["id"], msg["ring"]
        self.table_size, self.year = msg["table_size"], msg["year"]
        print(f"RECV reset-id: new ID={self.my_id}, ring size={len(self.ring)}, "
              f"leader={self.ring[0]['name']}, right neighbour={self.right()['name']}", flush=True)
        # The last node in the new ring hands the message back to its origin.
        nxt = msg["origin"] if self.my_id == len(self.ring) - 1 else self.ring[self.my_id + 1]
        self.send(nxt, "reset-id", origin=msg["origin"], token=msg["token"], id=self.my_id + 1,
                  ring=self.ring, table_size=self.table_size, year=self.year)

    def on_rebuild_dht(self, msg):
        print(f"RECV rebuild-dht from {msg['notify']['name']}: I am the new leader (ID 0)", flush=True)
        status = "SUCCESS"
        try:
            records, _ = load_records(msg["year"])
            self.build(records)
        except Exception as exc:
            status = "FAILURE"
            print(f"ERROR rebuilding: {exc}; deleting the partial DHT", flush=True)
            self.abandon_dht()
        self.send(msg["notify"], "rebuild-done", token=msg["token"], status_code=status,
                  new_leader=self.name, intact=False)

    def on_join_request(self, msg):
        joiner = msg["peer"]
        print(f"RECV join-request from {joiner['name']}", flush=True)
        # Check the dataset before touching the ring, so a missing file leaves
        # the existing DHT intact and the join is simply refused.
        try:
            records, _ = load_records(self.year)
        except Exception as exc:
            print(f"ERROR: cannot rebuild ({exc}); join refused, DHT unchanged", flush=True)
            self.send(joiner, "rebuild-done", token=msg["token"], status_code="FAILURE",
                      new_leader=self.name, intact=True)
            return
        status = "SUCCESS"
        try:
            self.ring_teardown(final=False)
            new_ring = self.ring + [joiner]
            self.ring_reset(new_ring, next_hop=new_ring[1], first_id=1)
            self.build(records)
        except Exception as exc:
            status = "FAILURE"
            print(f"ERROR during join: {exc}; deleting the partial DHT", flush=True)
            self.abandon_dht()
        self.send(joiner, "rebuild-done", token=msg["token"], status_code=status,
                  new_leader=self.name, intact=False)

    def abandon_dht(self):
        """After a failed rebuild, clear tables and ring state on every node."""
        try:
            if self.ring:
                self.ring_teardown(final=True)
        except Exception as exc:
            print(f"ERROR during cleanup teardown: {exc}", flush=True)
        with self.lock:
            self.table.clear()
        self.my_id, self.ring, self.table_size, self.year = None, [], None, None

    def on_find_event(self, msg):
        event_id, sender = msg["event_id"], msg["sender"]
        if self.my_id is None:
            self.send(sender, "find-event-result", token=msg["token"], status_code="FAILURE",
                      id_seq=msg.get("id_seq", []), reason="not part of a DHT")
            return
        n = len(self.ring)
        pos = event_id % self.table_size
        target = pos % n
        id_seq = msg.get("id_seq") or [self.my_id]
        remaining = msg.get("remaining")
        if remaining is None:                       # first node of the query
            remaining = [i for i in range(n) if i != self.my_id]
        print(f"RECV find-event {event_id} from {msg.get('from', sender['name'])}: "
              f"pos={pos}, id={target}, id-seq={id_seq}", flush=True)
        record = None
        if target == self.my_id:
            with self.lock:
                record = next((r for r in self.table.get(pos, [])
                               if int(r["event_id"]) == event_id), None)
        if record is not None or target == self.my_id or not remaining:
            status = "SUCCESS" if record is not None else "FAILURE"
            print(f"  find-event {event_id}: {status} at ID {self.my_id}; reply to {sender['name']}", flush=True)
            self.send(sender, "find-event-result", token=msg["token"], status_code=status,
                      record=record, id_seq=id_seq, event_id=event_id)
            return
        nxt = random.choice(remaining)               # hot potato: random next node
        remaining = [i for i in remaining if i != nxt]
        print(f"  hot potato: forward to ID {nxt} ({self.ring[nxt]['name']})", flush=True)
        self.send(self.ring[nxt], "find-event", event_id=event_id, sender=sender, token=msg["token"],
                  id_seq=id_seq + [nxt], remaining=remaining, **{"from": self.name})

    # ------------------------------------------------------------------ ring procedures
    def ring_teardown(self, final):
        """Generic teardown: send around the ring, delete own table on return."""
        token = self.new_waiter()
        self.send(self.right(), "teardown", origin=self.me(), token=token, final=final)
        self.wait_for(token, RING_WAIT)
        with self.lock:
            dropped = sum(len(b) for b in self.table.values())
            self.table.clear()
        print(f"RECV teardown back at origin: deleted own local hash table ({dropped} records)", flush=True)
        if final:
            self.my_id, self.ring, self.table_size, self.year = None, [], None, None

    def ring_reset(self, new_ring, next_hop, first_id):
        """Send reset-id along new_ring; it returns to this peer when done."""
        token = self.new_waiter()
        if first_id == 1:                       # this peer is ID 0 of the new ring
            self.my_id, self.ring = 0, new_ring
        self.send(next_hop, "reset-id", origin=self.me(), token=token, id=first_id,
                  ring=new_ring, table_size=self.table_size, year=self.year)
        self.wait_for(token, RING_WAIT)
        print(f"RECV reset-id back at origin: ring renumbered, size {len(new_ring)}, "
              f"leader {new_ring[0]['name']}", flush=True)

    def build(self, records):
        """Step 2 of setup: this peer (ID 0) distributes records along the ring."""
        n = len(self.ring)
        with self.lock:
            self.table.clear()
        print(f"Building local DHTs: {len(records)} records, s={self.table_size}, n={n}; "
              f"sending store messages to {self.right()['name']}", flush=True)
        sent = 0
        for record in records:
            pos = int(record["event_id"]) % self.table_size
            target = pos % n
            if target == 0:
                with self.lock:
                    self.table.setdefault(pos, []).append(record)
            else:
                self.send(self.right(), "store", quiet=True, target=target, pos=pos, record=record)
                sent += 1
        print(f"SENT {sent} store messages to {self.right()['name']}; "
              f"kept {len(records) - sent} locally", flush=True)
        # A count message follows the stores around the ring, so it arrives at
        # every node only after that node has handled every earlier store.
        token = self.new_waiter()
        self.send(self.right(), "count", origin=self.me(), token=token,
                  counts=[{"id": 0, "name": self.name, "count": self.local_count()}])
        counts = self.wait_for(token, RING_WAIT)["counts"]
        print("DHT configuration (records per node):", flush=True)
        for entry in counts:
            print(f"  Node {entry['id']} ({entry['name']}): {entry['count']} records", flush=True)
        total = sum(e["count"] for e in counts)
        print(f"  Total: {total} of {len(records)}", flush=True)
        if total != len(records):
            raise RuntimeError("record count mismatch")

    # ------------------------------------------------------------------ commands
    def register(self, name, ip, m_port, p_port):
        if self.name is not None:
            print("This process is already registered")
            return
        if not (name.isascii() and name.isalpha() and 1 <= len(name) <= 15):
            print("Name must contain 1-15 ASCII letters")
            return
        ipaddress.IPv4Address(ip)
        if not (PORT_MIN <= m_port <= PORT_MAX and PORT_MIN <= p_port <= PORT_MAX and m_port != p_port):
            print(f"Ports must be distinct and in {PORT_MIN}..{PORT_MAX}")
            return
        self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.m_sock.bind((ip, m_port))
            self.p_sock.bind((ip, p_port))
            self.m_sock.settimeout(TIMEOUT)
            print("SEND register to manager", flush=True)
            answer = exchange(self.m_sock, self.manager,
                              {"command": "register", "name": name, "ip": ip,
                               "m_port": m_port, "p_port": p_port})
            print(f"RECV register response: {answer['status']} {answer.get('reason', '')}", flush=True)
            if answer["status"] != "SUCCESS":
                self.m_sock.close()
                self.p_sock.close()
                self.m_sock = self.p_sock = None
                return
            self.name, self.ip, self.p_port = name, ip, p_port
            threading.Thread(target=self.receive_loop, daemon=True).start()
            threading.Thread(target=self.work_loop, daemon=True).start()
            print(f"REGISTERED {name}, manager port={m_port}, peer port={p_port}", flush=True)
        except Exception:
            self.m_sock.close()
            self.p_sock.close()
            self.m_sock = self.p_sock = None
            raise

    def setup(self, n, year):
        # Check the dataset before locking the manager into its setup state.
        records, size = load_records(year)
        answer = self.manager_request("setup-dht", n=n, year=year)
        if answer["status"] != "SUCCESS":
            return
        self.ring, self.my_id, self.table_size, self.year = answer["peers"], 0, size, year
        print(f"I am the leader (ID 0); ring: "
              + ", ".join(f"{i}:{p['name']}" for i, p in enumerate(self.ring)), flush=True)
        for index, peer in enumerate(self.ring[1:], 1):
            self.send(peer, "set-id", id=index, ring=self.ring, table_size=size, year=year)
        self.build(records)
        self.manager_request("dht-complete")

    def query(self, event_id):
        answer = self.manager_request("query-dht")
        if answer["status"] != "SUCCESS":
            return
        start = answer["peer"]
        print(f"Query starts at {start['name']}", flush=True)
        token = self.new_waiter()
        self.send(start, "find-event", event_id=event_id, sender=self.me(), token=token)
        result = self.wait_for(token, QUERY_WAIT)
        print(f"RECV find-event-result: {result['status_code']}", flush=True)
        if result["status_code"] == "SUCCESS":
            record = result["record"]
            print("Storm event found:")
            width = max(len(f) for f in FIELDS)
            for field in FIELDS:
                print(f"  {field:<{width}} : {record.get(field, '')}")
        else:
            print(f"Storm event {event_id} not found in the DHT.")
        print(f"  id-seq (nodes probed): {result.get('id_seq')}", flush=True)

    def leave(self):
        if self.my_id is None:
            print("Not part of a DHT (the manager will also refuse)", flush=True)
        answer = self.manager_request("leave-dht")
        if answer["status"] != "SUCCESS":
            return
        i = self.my_id
        new_ring = self.ring[i + 1:] + self.ring[:i]
        print("Step 1: teardown of the DHT", flush=True)
        self.ring_teardown(final=False)
        print("Step 2: renumbering the ring with reset-id", flush=True)
        self.ring_reset(new_ring, next_hop=new_ring[0], first_id=0)
        print(f"Step 3: rebuild-dht to new leader {new_ring[0]['name']}", flush=True)
        token = self.new_waiter()
        self.send(new_ring[0], "rebuild-dht", notify=self.me(), token=token, year=self.year)
        done = self.wait_for(token, RING_WAIT)
        print(f"RECV rebuild-done from {done['new_leader']}: {done['status_code']}", flush=True)
        self.my_id, self.ring, self.table_size, self.year = None, [], None, None
        print("Step 4: notify manager", flush=True)
        self.report_rebuilt(done)

    def join(self):
        answer = self.manager_request("join-dht")
        if answer["status"] != "SUCCESS":
            return
        leader = answer["leader"]
        token = self.new_waiter()
        self.send(leader, "join-request", peer=self.me(), token=token)
        done = self.wait_for(token, RING_WAIT)
        print(f"RECV rebuild-done from {done['new_leader']}: {done['status_code']}; "
              f"my ID={self.my_id}, ring size={len(self.ring)}", flush=True)
        if done["status_code"] != "SUCCESS":
            self.my_id, self.ring, self.table_size, self.year = None, [], None, None
        self.report_rebuilt(done)

    def report_rebuilt(self, done):
        """Send dht-rebuilt; on a failed rebuild, tell the manager so it does
        not treat an empty or partial DHT as available for queries."""
        if done["status_code"] == "SUCCESS":
            self.manager_request("dht-rebuilt", new_leader=done["new_leader"])
        else:
            print("WARNING: rebuild failed; reporting the failure to the manager", flush=True)
            self.manager_request("dht-rebuilt", new_leader=done["new_leader"],
                                 result="FAILURE", intact=bool(done.get("intact")))

    def teardown(self):
        answer = self.manager_request("teardown-dht")
        if answer["status"] != "SUCCESS":
            return
        self.ring_teardown(final=True)
        self.manager_request("teardown-complete")

    def deregister(self):
        answer = self.manager_request("deregister")
        if answer["status"] == "SUCCESS":
            print(f"{self.name} deregistered; exiting.", flush=True)
            self.m_sock.close()
            self.p_sock.close()
            return True
        return False


USAGE = ("Commands:\n"
         "  register <name> <IPv4> <m-port> <p-port>\n"
         "  setup-dht <name> <n> <YYYY>\n"
         "  query-dht <name> [event-id]   (prompts for event-id if omitted)\n"
         "  leave-dht <name> | join-dht <name> | teardown-dht <name>\n"
         "  deregister <name> | quit")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("manager_ip")
    parser.add_argument("manager_port", type=int)
    args = parser.parse_args()
    ipaddress.IPv4Address(args.manager_ip)
    if not PORT_MIN <= args.manager_port <= PORT_MAX:
        parser.error(f"manager port must be in {PORT_MIN}..{PORT_MAX}")
    peer = Peer((args.manager_ip, args.manager_port))
    print(USAGE, flush=True)
    while True:
        try:
            parts = input("peer> ").split()
            if not parts:
                continue
            cmd = parts[0]
            if cmd == "quit":
                break
            if cmd == "register" and len(parts) == 5:
                peer.register(parts[1], parts[2], int(parts[3]), int(parts[4]))
                continue
            if cmd not in {"setup-dht", "query-dht", "leave-dht", "join-dht",
                           "teardown-dht", "deregister"} or len(parts) < 2:
                print("Unknown command or wrong number of arguments\n" + USAGE)
                continue
            if peer.name is None:
                print("Register first")
                continue
            if parts[1] != peer.name:
                print("Commands must use this process's registered name")
                continue
            if cmd == "setup-dht" and len(parts) == 4:
                peer.setup(int(parts[2]), int(parts[3]))
            elif cmd == "query-dht" and len(parts) in (2, 3):
                event_id = parts[2] if len(parts) == 3 else input("event-id> ")
                peer.query(int(event_id))
            elif cmd == "leave-dht" and len(parts) == 2:
                peer.leave()
            elif cmd == "join-dht" and len(parts) == 2:
                peer.join()
            elif cmd == "teardown-dht" and len(parts) == 2:
                peer.teardown()
            elif cmd == "deregister" and len(parts) == 2:
                if peer.deregister():
                    break
            else:
                print("Wrong number of arguments\n" + USAGE)
        except (OSError, ValueError, TimeoutError, RuntimeError, KeyError) as exc:
            print(f"ERROR: {exc}", flush=True)
        except (KeyboardInterrupt, EOFError):
            break
    sys.exit(0)


if __name__ == "__main__":
    main()
