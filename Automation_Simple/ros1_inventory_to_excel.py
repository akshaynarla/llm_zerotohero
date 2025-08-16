#!/usr/bin/env python3
"""
ros1_inventory_to_excel.py
 
Vibe coded automation script to assist migration from ROS1 Noetic to ROS2 Jazzy.
Exports from ROS1 environment (all data: nodes, services, topics etc.) used as input

Run:
  python3 ros1_inventory_to_excel.py --in-dir /path/to/discovery --out /path/for/output
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Set

import pandas as pd

try:
    import yaml
except Exception:
    yaml = None

# ---------- small utils ----------

def read_lines(p: Path) -> List[str]:
    try:
        return p.read_text(errors="ignore").splitlines()
    except Exception:
        return []

def ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    extra = [c for c in df.columns if c not in cols]
    return df[cols + extra]

def nonempty(df: Optional[pd.DataFrame]) -> bool:
    return isinstance(df, pd.DataFrame) and not df.empty

def choose_excel_engine() -> str:
    try:
        import openpyxl  # noqa: F401
        return "openpyxl"
    except Exception:
        pass
    try:
        import xlsxwriter  # noqa: F401
        return "xlsxwriter"
    except Exception:
        raise RuntimeError("Install an Excel engine: pip install openpyxl OR pip install xlsxwriter")

# ---------- debug collector ----------

class Debug:
    def __init__(self) -> None:
        self.lines: List[str] = []
    def add(self, msg: str) -> None:
        self.lines.append(msg)
    def add_block(self, title: str, items: List[str], max_items: int = 10) -> None:
        self.lines.append(f"\n== {title} ==")
        for s in items[:max_items]:
            self.lines.append(s)
        if len(items) > max_items:
            self.lines.append(f"... (+{len(items)-max_items} more)")
    def write(self, path: Path) -> None:
        path.write_text("\n".join(self.lines))

dbg = Debug()

# ---------- structured: inventory.json (if present) ----------

def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None

def parse_inventory(inv_path: Path) -> Dict[str, pd.DataFrame]:
    data = load_json(inv_path)
    if not isinstance(data, dict):
        dbg.add("inventory.json present but not a dict; skipping.")
        return {}
    dbg.add("Parsed inventory.json")

    # Nodes
    nodes = []
    for n in data.get("nodes") or data.get("node_list") or []:
        if isinstance(n, dict):
            nodes.append({
                "node": n.get("name",""),
                "pkg": n.get("pkg",""),
                "exe": n.get("exe",""),
                "type": n.get("type",""),
                "pid": n.get("pid",""),
                "machine": n.get("machine") or n.get("host") or "",
                "ros_namespace": n.get("namespace",""),
                "is_nodelet": str(n.get("is_nodelet", False)),
                "notes": n.get("notes",""),
            })
        elif isinstance(n, str):
            nodes.append({"node": n})
    nodes_df = ensure_columns(pd.DataFrame(nodes), ["node","pkg","exe","type","pid","machine","ros_namespace","is_nodelet","notes"])

    # Topics
    topics = []
    for t in data.get("topics") or []:
        if isinstance(t, dict):
            pubs = t.get("publishers", []) or t.get("pubs", []) or []
            subs = t.get("subscribers", []) or t.get("subs", []) or []
            topics.append({
                "topic": t.get("name",""),
                "type": t.get("type",""),
                "publishers": ",".join(pubs),
                "subscribers": ",".join(subs),
                "latched": str(t.get("latched","")),
                "bw_est": t.get("bandwidth",""),
                "hz_est": t.get("rate",""),
                "notes": t.get("notes",""),
            })
        elif isinstance(t, str):
            topics.append({"topic": t})
    topics_df = ensure_columns(pd.DataFrame(topics), ["topic","type","publishers","subscribers","latched","bw_est","hz_est","notes"])

    # Services
    services = []
    for s in data.get("services") or []:
        if isinstance(s, dict):
            providers = s.get("providers", []) or s.get("nodes", []) or []
            services.append({
                "service": s.get("name",""),
                "type": s.get("type",""),
                "providers": ",".join(providers),
                "notes": s.get("notes",""),
            })
        elif isinstance(s, str):
            services.append({"service": s})
    services_df = ensure_columns(pd.DataFrame(services), ["service","type","providers","notes"])

    # Actions (optional)
    actions = []
    for a in data.get("actions") or data.get("inferred_actions") or []:
        if isinstance(a, dict):
            actions.append({
                "action_ns": a.get("name",""),
                "type": a.get("type",""),
                "provider": a.get("server","") or a.get("node",""),
                "clients": ",".join(a.get("clients",[]) or []),
                "notes": a.get("notes",""),
            })
        elif isinstance(a, str):
            actions.append({"action_ns": a})
    actions_df = ensure_columns(pd.DataFrame(actions), ["action_ns","type","provider","clients","notes"])

    out = {}
    if nonempty(nodes_df): out["Nodes"] = nodes_df
    if nonempty(topics_df): out["Topics"] = topics_df
    if nonempty(services_df): out["Services"] = services_df
    if nonempty(actions_df): out["Actions"] = actions_df
    return out

# ---------- raw: tolerant parsers with multiple patterns ----------

def tolerant_topics_from_topics_verbose(path: Path) -> pd.DataFrame:
    lines = path.read_text(errors="ignore").splitlines()
    if not lines:
        return pd.DataFrame()

    # Accept bullet lines with optional [type] and trailing counts
    topic_line = re.compile(r"^\s*[*-]\s*([/][^\s]+)\s*(?:\[(.+?)\])?")
    pubs: Dict[str, set] = {}
    subs: Dict[str, set] = {}
    types: Dict[str, str] = {}
    section = None
    current_topic = None

    for ln in lines:
        if "Published topics" in ln:
            section = "pub"; current_topic = None; continue
        if "Subscribed topics" in ln:
            section = "sub"; current_topic = None; continue

        m = topic_line.match(ln)
        if m:
            current_topic = m.group(1).strip()
            t = (m.group(2) or "").strip()
            if t:
                types[current_topic] = t
            # When no node list follows, we still record the topic but not edges.
            pubs.setdefault(current_topic, set())
            subs.setdefault(current_topic, set())
            continue

        # Node lines (rare in your dump) usually start with a leading slash
        if current_topic and ln.strip().startswith("/"):
            if section == "pub":
                pubs[current_topic].add(ln.strip())
            elif section == "sub":
                subs[current_topic].add(ln.strip())

    rows = []
    for topic in sorted(set(pubs) | set(subs)):
        rows.append({
            "topic": topic,
            "type": types.get(topic, ""),
            "publishers": ",".join(sorted(pubs.get(topic, set()))),
            "subscribers": ",".join(sorted(subs.get(topic, set()))),
            "latched": "", "bw_est": "", "hz_est": "", "notes": "",
        })
    return ensure_columns(pd.DataFrame(rows), ["topic","type","publishers","subscribers","latched","bw_est","hz_est","notes"])

def tolerant_nodes_from_nodes_info(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    lines = read_lines(path)
    if not lines:
        dbg.add("nodes_info.txt empty or unreadable")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    node_hdr1 = re.compile(r"^\s*Node\s+\[([^\]]+)\]\s*$")  # Node [/foo]
    node_hdr2 = re.compile(r"^\s*Node:\s*([/]\S+)\s*$")     # Node: /foo
    list_item = re.compile(r"^\s*[*-]\s*([/][^\s]+)\s*(?:\[(.+?)\])?.*$")

    cur_node: Optional[str] = None
    section: Optional[str] = None
    pubs: Dict[str, Set[str]] = {}
    subs: Dict[str, Set[str]] = {}
    svc_providers: Dict[str, Set[str]] = {}
    topic_types: Dict[str, str] = {}
    node_meta: Dict[str, Dict[str, Any]] = {}
    examples: List[str] = []

    for ln in lines:
        m1 = node_hdr1.match(ln) or node_hdr2.match(ln)
        if m1:
            cur_node = m1.group(1).strip()
            section = None
            node_meta.setdefault(cur_node, {})
            examples.append(ln.strip())
            continue

        if cur_node:
            # section switches
            if re.match(r"^\s*Publications:", ln, re.I): section = "pub"; continue
            if re.match(r"^\s*Subscriptions:", ln, re.I): section = "sub"; continue
            if re.match(r"^\s*Services:", ln, re.I): section = "svc"; continue

            # key: value meta lines (pid, machine, etc.)
            kv = re.match(r"^\s*([A-Za-z ]+):\s*(.*)\s*$", ln)
            if kv and section is None:
                node_meta[cur_node][kv.group(1).strip().lower().replace(" ","_")] = kv.group(2).strip()
                continue

            mli = list_item.match(ln)
            if mli and section:
                item = mli.group(1).strip()
                typ = (mli.group(2) or "").strip()
                if section == "pub":
                    pubs.setdefault(item, set()).add(cur_node)
                    if typ: topic_types[item] = typ
                elif section == "sub":
                    subs.setdefault(item, set()).add(cur_node)
                    if typ: topic_types[item] = typ
                elif section == "svc":
                    svc_providers.setdefault(item, set()).add(cur_node)

    topics = sorted(set(pubs.keys()) | set(subs.keys()))
    nodes = sorted(set(node_meta.keys()) | set().union(*pubs.values()) | set().union(*subs.values()) if pubs or subs else set(node_meta.keys()))

    dbg.add(f"nodes_info: nodes={len(nodes)} topics={len(topics)} svc={len(svc_providers)}")
    dbg.add_block("nodes_info examples", examples)

    nodes_df = ensure_columns(pd.DataFrame([{
        "node": n,
        "pkg": node_meta.get(n,{}).get("pkg",""),
        "exe": node_meta.get(n,{}).get("exe",""),
        "type": node_meta.get(n,{}).get("type",""),
        "pid": node_meta.get(n,{}).get("pid",""),
        "machine": node_meta.get(n,{}).get("machine","") or node_meta.get(n,{}).get("host",""),
        "ros_namespace": node_meta.get(n,{}).get("namespace",""),
        "is_nodelet": node_meta.get(n,{}).get("is_nodelet",""),
        "notes": "",
    } for n in nodes]), ["node","pkg","exe","type","pid","machine","ros_namespace","is_nodelet","notes"])

    topics_df = ensure_columns(pd.DataFrame([{
        "topic": t,
        "type": topic_types.get(t,""),
        "publishers": ",".join(sorted(pubs.get(t,set()))),
        "subscribers": ",".join(sorted(subs.get(t,set()))),
        "latched": "", "bw_est": "", "hz_est": "", "notes": "",
    } for t in topics]), ["topic","type","publishers","subscribers","latched","bw_est","hz_est","notes"])

    services_df = ensure_columns(pd.DataFrame([{
        "service": s,
        "type": "",
        "providers": ",".join(sorted(nodes)),
        "notes": "",
    } for s, nodes in svc_providers.items()]), ["service","type","providers","notes"])

    return nodes_df, topics_df, services_df

def tolerant_services_from_services_verbose(path: Path) -> pd.DataFrame:
    lines = read_lines(path)
    if not lines:
        dbg.add("services_verbose.txt empty or unreadable")
        return pd.DataFrame()

    service_line = re.compile(r"^\s*([/][\w/]+)\s*$")
    node_item = re.compile(r".*Node:\s*([/]\S+)", re.I)
    type_item = re.compile(r".*Type:\s*([\w/]+)", re.I)
    advertised = re.compile(r".*advertised by\s*([/]\S+)", re.I)

    cur: Optional[str] = None
    cur_type: Optional[str] = None
    prov: Set[str] = set()
    table: Dict[str, Dict[str, Any]] = {}
    examples: List[str] = []

    def flush():
        nonlocal cur, cur_type, prov
        if cur:
            rec = table.setdefault(cur, {"type": cur_type or "", "providers": set()})
            rec["providers"].update(prov)
        cur = None; cur_type = None; prov = set()

    for ln in lines:
        m_srv = service_line.match(ln)
        if m_srv:
            flush()
            cur = m_srv.group(1).strip()
            examples.append(ln.strip())
            continue
        m_node = node_item.match(ln)
        if m_node:
            prov.add(m_node.group(1).strip()); continue
        m_adv = advertised.match(ln)
        if m_adv:
            prov.add(m_adv.group(1).strip()); continue
        m_type = type_item.match(ln)
        if m_type:
            cur_type = m_type.group(1).strip(); continue
    flush()

    rows = [{
        "service": s,
        "type": rec.get("type",""),
        "providers": ",".join(sorted(rec.get("providers", set()))),
        "notes": "",
    } for s, rec in table.items()]
    dbg.add(f"services_verbose: services={len(rows)}")
    dbg.add_block("services_verbose examples", examples)
    return ensure_columns(pd.DataFrame(rows), ["service","type","providers","notes"])

# ---------- fusion & edges ----------

def merge_topics(primary: pd.DataFrame, secondary: pd.DataFrame) -> pd.DataFrame:
    if not nonempty(primary): return secondary.copy()
    if not nonempty(secondary): return primary.copy()
    p = primary.set_index("topic", drop=False)
    s = secondary.set_index("topic", drop=False)
    rows = []
    for t in sorted(set(p.index)|set(s.index)):
        pr = p.loc[t] if t in p.index else None
        sr = s.loc[t] if t in s.index else None
        typ = pr.get("type","") if pr is not None and pr.get("type","") else (sr.get("type","") if sr is not None else "")
        pubs = sorted(set((str(pr.get("publishers","")).split(",") if pr is not None else []) +
                          (str(sr.get("publishers","")).split(",") if sr is not None else [])) - {""})
        subs = sorted(set((str(pr.get("subscribers","")).split(",") if pr is not None else []) +
                          (str(sr.get("subscribers","")).split(",") if sr is not None else [])) - {""})
        rows.append({
            "topic": t, "type": typ,
            "publishers": ",".join(pubs), "subscribers": ",".join(subs),
            "latched":"", "bw_est":"", "hz_est":"", "notes":""
        })
    return ensure_columns(pd.DataFrame(rows), ["topic","type","publishers","subscribers","latched","bw_est","hz_est","notes"])

def edges_from_topics(topics_df: pd.DataFrame, col: str) -> pd.DataFrame:
    rows = []
    for _, r in topics_df.iterrows():
        top = r["topic"]
        for n in [x.strip() for x in str(r.get(col,"")).split(",") if x.strip()]:
            rows.append({"node": n, "topic": top})
    return ensure_columns(pd.DataFrame(rows), ["node","topic"])

ACTION_SUFFIXES = ["goal","status","feedback","cancel","result"]

def infer_actions(topics_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not nonempty(topics_df): return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    topic_to_pubs = {r["topic"]: [x for x in str(r.get("publishers","")).split(",") if x] for _, r in topics_df.iterrows()}
    topic_to_subs = {r["topic"]: [x for x in str(r.get("subscribers","")).split(",") if x] for _, r in topics_df.iterrows()}
    ns_map: Dict[str, Dict[str, Set[str]]] = {}
    for t in topic_to_pubs.keys() | topic_to_subs.keys():
        for suf in ACTION_SUFFIXES:
            if t.endswith("/"+suf):
                ns = t[:-(len(suf)+1)]
                ns_map.setdefault(ns, {}).setdefault(suf, set()).add(t)
    arows, srows, crows = [], [], []
    for ns, groups in ns_map.items():
        servers, clients = set(), set()
        for suf in ("status","feedback","result"):
            for t in groups.get(suf, []):
                servers.update(topic_to_pubs.get(t, []))
                clients.update(topic_to_subs.get(t, []))
        for suf in ("goal","cancel"):
            for t in groups.get(suf, []):
                clients.update(topic_to_pubs.get(t, []))
                servers.update(topic_to_subs.get(t, []))
        # best-effort type
        tmap = {r["topic"]: r.get("type","") for _, r in topics_df.iterrows()}
        types = {tmap.get(t,"") for ts in groups.values() for t in ts if tmap.get(t,"")}
        arows.append({"action_ns": ns, "type": sorted(types)[0] if types else "", "provider": ",".join(sorted(servers)), "clients": ",".join(sorted(clients)), "notes": "inferred"})
        for n in sorted(servers): srows.append({"node": n, "action_ns": ns})
        for n in sorted(clients): crows.append({"node": n, "action_ns": ns})
    return (ensure_columns(pd.DataFrame(arows), ["action_ns","type","provider","clients","notes"]),
            ensure_columns(pd.DataFrame(srows), ["node","action_ns"]),
            ensure_columns(pd.DataFrame(crows), ["node","action_ns"]))

def node_io(pubs_df: pd.DataFrame, subs_df: pd.DataFrame) -> pd.DataFrame:
    pubmap = pubs_df.groupby("node")["topic"].apply(lambda s: ",".join(sorted(set(s)))).to_dict() if nonempty(pubs_df) else {}
    submap = subs_df.groupby("node")["topic"].apply(lambda s: ",".join(sorted(set(s)))).to_dict() if nonempty(subs_df) else {}
    nodes = sorted(set(pubmap) | set(submap))
    return ensure_columns(pd.DataFrame([{"node": n, "published_topics": pubmap.get(n,""), "subscribed_topics": submap.get(n,"")} for n in nodes]),
                          ["node","published_topics","subscribed_topics"])

def rollup(nodes_df: pd.DataFrame, topics_df: pd.DataFrame, services_df: pd.DataFrame,
           act_serv_df: pd.DataFrame, act_client_df: pd.DataFrame) -> pd.DataFrame:
    pubs = edges_from_topics(topics_df, "publishers") if nonempty(topics_df) else pd.DataFrame(columns=["node","topic"])
    subs = edges_from_topics(topic_df := topics_df, "subscribers") if nonempty(topics_df) else pd.DataFrame(columns=["node","topic"])
    pub_counts = pubs.groupby("node").size().to_dict() if nonempty(pubs) else {}
    sub_counts = subs.groupby("node").size().to_dict() if nonempty(subs) else {}
    svc_nodes = set()
    if nonempty(services_df):
        for _, r in services_df.iterrows():
            for n in [x.strip() for x in str(r.get("providers","")).split(",") if x.strip()]:
                svc_nodes.add(n)
    act_serv_nodes = set(act_serv_df["node"].tolist()) if nonempty(act_serv_df) else set()
    act_client_counts = act_client_df.groupby("node").size().to_dict() if nonempty(act_client_df) else {}

    all_nodes = set(nodes_df["node"].tolist()) if nonempty(nodes_df) else set(pub_counts) | set(sub_counts) | svc_nodes | set(act_serv_nodes) | set(act_client_counts)
    rows = []
    ndx = nodes_df.set_index("node") if nonempty(nodes_df) else pd.DataFrame().set_index(pd.Index([]))
    for n in sorted(all_nodes):
        rows.append({
            "node": n,
            "pkg": ndx.get("pkg", {}).get(n, "") if nonempty(nodes_df) else "",
            "machine": ndx.get("machine", {}).get(n, "") if nonempty(nodes_df) else "",
            "pub_topics": pub_counts.get(n, 0),
            "sub_topics": sub_counts.get(n, 0),
            "provides_service": int(n in svc_nodes),
            "is_action_server": int(n in act_serv_nodes),
            "is_action_client": act_client_counts.get(n, 0),
        })
    return ensure_columns(pd.DataFrame(rows),
                          ["node","pkg","machine","pub_topics","sub_topics","provides_service","is_action_server","is_action_client"])

def plan(roll: pd.DataFrame) -> pd.DataFrame:
    df = roll.copy()
    def score(r):
        s = 0
        if (int(r.get("pub_topics",0))==0) ^ (int(r.get("sub_topics",0))==0): s += 1
        if not int(r.get("provides_service",0)) and not int(r.get("is_action_server",0)) and not int(r.get("is_action_client",0)): s += 1
        return s
    df["priority_score"] = df.apply(score, axis=1)
    df["priority_bucket"] = pd.cut(df["priority_score"], bins=[-1,0,1,2,3], labels=["Hard","Medium","Easy","Very Easy"])
    for c in ["ros2_pkg","conversion_status","owner","notes"]:
        df[c] = "" if c!="conversion_status" else "todo"
    return ensure_columns(df, ["node","pkg","machine","priority_score","priority_bucket","ros2_pkg","conversion_status","owner","notes","pub_topics","sub_topics","provides_service","is_action_server","is_action_client"])

def infer_missing_namespaces(nodes_df: pd.DataFrame) -> pd.DataFrame:
    if "ros_namespace" not in nodes_df.columns:
        nodes_df["ros_namespace"] = ""
    def ns_from_node(node: str) -> str:
        # '/a/b/c' -> '/a/b'; '/' or '/node' -> '/'
        if not node or not node.startswith("/"):
            return ""
        parts = node.strip("/").split("/")
        if len(parts) <= 1:
            return "/"
        return "/" + "/".join(parts[:-1])
    mask = (nodes_df["ros_namespace"].astype(str).str.len() == 0) & (
        nodes_df["node"].astype(str).str.startswith("/")
    )
    nodes_df.loc[mask, "ros_namespace"] = nodes_df.loc[mask, "node"].apply(ns_from_node)
    return nodes_df

# ---------- parameters (optional) ----------

def parse_params_yaml(path: Path) -> pd.DataFrame:
    if yaml is None:
        dbg.add("PyYAML not installed; skipping params.yaml")
        return pd.DataFrame()
    tree = None
    try:
        tree = yaml.safe_load(path.read_text())
    except Exception:
        dbg.add("Failed to parse params.yaml (PyYAML error)")
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    def walk(prefix, obj):
        if isinstance(obj, dict):
            for k, v in obj.items(): walk(prefix + "/" + str(k) if prefix else "/" + str(k), v)
        elif isinstance(obj, list):
            for i, v in enumerate(obj): walk(prefix + f"/{i}", v)
        else:
            rows.append({"param": prefix or "/", "value": obj})
    if isinstance(tree, (dict,list)): walk("", tree)
    dbg.add(f"params.yaml: entries={len(rows)}")
    return ensure_columns(pd.DataFrame(rows), ["param","value"])

def best_effort_bind_params(params_df: pd.DataFrame, nodes_df: pd.DataFrame) -> pd.DataFrame:
    if not (nonempty(params_df) and nonempty(nodes_df) and "ros_namespace" in nodes_df.columns):
        return pd.DataFrame()
    ns_map = {r["node"]: str(r["ros_namespace"]) for _, r in nodes_df.iterrows()}
    rows = []
    for node, ns in ns_map.items():
        if not ns: continue
        pref = ns if ns.startswith("/") else "/" + ns
        cand = params_df.loc[params_df["param"].astype(str).str.startswith(pref)]
        for _, r in cand.iterrows():
            rows.append({"node": node, "param": r["param"], "value": r.get("value","")})
    dbg.add(f"ParamBindings: entries={len(rows)}")
    return ensure_columns(pd.DataFrame(rows), ["node","param","value"])

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True)
    ap.add_argument("--out", default=str(Path.cwd()))
    ap.add_argument("--xlsx", default="ros_migration_inventory.xlsx")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = out_dir / args.xlsx
    dbg_path = out_dir / "debug_report.txt"

    sheets: Dict[str, pd.DataFrame] = {}

    # inventory.json first
    inv = in_dir / "inventory.json"
    if inv.exists():
        sheets.update(parse_inventory(inv))
    else:
        dbg.add("inventory.json not found; relying on raw dumps")

    # topics_verbose
    tv = in_dir / "topics_verbose.txt"
    topics_tv = tolerant_topics_from_topics_verbose(tv) if tv.exists() else pd.DataFrame()

    # nodes_info
    ni = in_dir / "nodes_info.txt"
    nodes_ni, topics_ni, services_ni = tolerant_nodes_from_nodes_info(ni) if ni.exists() else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    # services_verbose
    sv = in_dir / "services_verbose.txt"
    services_sv = tolerant_services_from_services_verbose(sv) if sv.exists() else pd.DataFrame()

    # Fuse Topics
    topics_df = sheets.get("Topics", pd.DataFrame())
    if nonempty(topics_tv): topics_df = merge_topics(topics_df, topics_tv) if nonempty(topics_df) else topics_tv
    if nonempty(topics_ni): topics_df = merge_topics(topics_df, topics_ni) if nonempty(topics_df) else topics_ni
    if nonempty(topics_df): sheets["Topics"] = topics_df

    # Nodes
    nodes_df = sheets.get("Nodes", pd.DataFrame())
    if not nonempty(nodes_df) and nonempty(nodes_ni):
        nodes_df = nodes_ni
    if not nonempty(nodes_df) and nonempty(topics_df):
        # build nodes from topic pubs/subs
        nodes = set()
        for _, r in topics_df.iterrows():
            nodes.update([x for x in str(r.get("publishers","")).split(",") if x])
            nodes.update([x for x in str(r.get("subscribers","")).split(",") if x])
        nodes_df = ensure_columns(pd.DataFrame([{"node": n, "pkg":"","exe":"","type":"","pid":"","machine":"","ros_namespace":"","is_nodelet":"","notes":""} for n in sorted(nodes)]),
                                  ["node","pkg","exe","type","pid","machine","ros_namespace","is_nodelet","notes"])
    if nonempty(nodes_df):
        nodes_df = infer_missing_namespaces(nodes_df)
        sheets["Nodes"] = nodes_df

    # Services
    services_df = sheets.get("Services", pd.DataFrame())
    if nonempty(services_ni):
        services_df = pd.concat([services_df, services_ni], ignore_index=True) if nonempty(services_df) else services_ni
    if nonempty(services_sv):
        if nonempty(services_df):
            base = services_df.set_index("service", drop=False)
            extra = services_sv.set_index("service", drop=False)
            rows = []
            for svc in sorted(set(base.index)|set(extra.index)):
                br = base.loc[svc] if svc in base.index else None
                er = extra.loc[svc] if svc in extra.index else None
                stype = br.get("type","") if br is not None and br.get("type","") else (er.get("type","") if er is not None else "")
                provs = sorted(set((str(br.get("providers","")).split(",") if br is not None else []) +
                                   (str(er.get("providers","")).split(",") if er is not None else [])) - {""})
                rows.append({"service": svc, "type": stype, "providers": ",".join(provs), "notes": ""})
            services_df = ensure_columns(pd.DataFrame(rows), ["service","type","providers","notes"])
        else:
            services_df = services_sv
    if nonempty(services_df): sheets["Services"] = services_df

    # Edges from Topics
    pubs_df = edges_from_topics(topics_df, "publishers") if nonempty(topics_df) else pd.DataFrame()
    subs_df = edges_from_topics(topics_df, "subscribers") if nonempty(topics_df) else pd.DataFrame()
    if nonempty(pubs_df): sheets["NodePublishesTopic"] = pubs_df
    if nonempty(subs_df): sheets["NodeSubscribesTopic"] = subs_df

    # Services edges
    if nonempty(services_df):
        rows = []
        for _, r in services_df.iterrows():
            svc = r["service"]
            for n in [x.strip() for x in str(r.get("providers","")).split(",") if x.strip()]:
                rows.append({"node": n, "service": svc})
        svc_edges = ensure_columns(pd.DataFrame(rows), ["node","service"])
        if nonempty(svc_edges): sheets["NodeProvidesService"] = svc_edges

    # Infer Actions from topics
    act_df, act_serv_df, act_client_df = infer_actions(topics_df) if nonempty(topics_df) else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    if nonempty(act_df): sheets["Actions"] = act_df
    if nonempty(act_serv_df): sheets["NodeIsActionServer"] = act_serv_df
    if nonempty(act_client_df): sheets["NodeIsActionClient"] = act_client_df

    # NodeIO + Rollup + Plan
    if nonempty(pubs_df) or nonempty(subs_df):
        nio = node_io(pubs_df, subs_df)
        if nonempty(nio): sheets["NodeIO"] = nio
    ru = rollup(nodes_df, topics_df, services_df, act_serv_df, act_client_df)
    if nonempty(ru): sheets["NodeRollup"] = ru
    mp = plan(ru)
    if nonempty(mp): sheets["MigrationPlan"] = mp

    # Parameters + best-effort bindings
    params = pd.DataFrame()
    py = in_dir / "params.yaml"
    if py.exists():
        params = parse_params_yaml(py)
        if nonempty(params): sheets["Parameters"] = params
        pb = best_effort_bind_params(params, nodes_df)
        if nonempty(pb): sheets["ParamBindings"] = pb

    # README
    if sheets:
        purpose = {
            "Nodes":"Nodes (from inventory or reconstructed).",
            "Topics":"Topics fused from dumps.",
            "Services":"Service types/providers fused.",
            "Actions":"Inferred action namespaces.",
            "NodePublishesTopic":"Edges: node->topic (publisher).",
            "NodeSubscribesTopic":"Edges: node->topic (subscriber).",
            "NodeProvidesService":"Edges: node->service (provider).",
            "NodeIsActionServer":"Edges: node->action_ns (server).",
            "NodeIsActionClient":"Edges: node->action_ns (client).",
            "NodeIO":"Per-node IO summary.",
            "NodeRollup":"Counts + roles.",
            "MigrationPlan":"Priority & tracking.",
            "Parameters":"Flattened params.",
            "ParamBindings":"Params bound to nodes by namespace.",
        }
        names = list(sheets.keys())
        readme = pd.DataFrame({"Sheet": names, "Purpose": [purpose.get(n,"") for n in names]})
    else:
        readme = pd.DataFrame()

    # Write Excel
    engine = choose_excel_engine()
    with pd.ExcelWriter(xlsx_path, engine=engine) as xl:
        for name, df in sheets.items():
            if nonempty(df):
                df.to_excel(xl, sheet_name=name[:31], index=False)
        if nonempty(readme):
            readme.to_excel(xl, sheet_name="README", index=False)

    # Write debug report
    if not sheets:
        dbg.add("No non-empty tables produced. This means the dump formats didn't match the tolerant parsers.")
        dbg.add("Please share nodes_info.txt / topics_verbose.txt / services_verbose.txt for exact tuning.")
    dbg.write(dbg_path)
    print(f"Wrote: {xlsx_path}")
    print(f"Debug: {dbg_path}")

if __name__ == "__main__":
    main()