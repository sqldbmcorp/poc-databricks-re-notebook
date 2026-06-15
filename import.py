# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks DDL  →  SqlDBM   (interactive)
# MAGIC
# MAGIC **Configure your Source DDL**
# MAGIC 1. Select source catalog  (Cell 2)
# MAGIC 2. Select source schema(s) (Cell 4)
# MAGIC 3. Generate DDL            (Cell 6)
# MAGIC
# MAGIC **Configure your Destination Project** (Cell 9 — interactive form)
# MAGIC 1. API Token (masked)
# MAGIC 2. Project: *Create new project* or pick an existing one
# MAGIC 3. Branch: shown only for Concurrent-Working projects — pick a branch or create a new one
# MAGIC 4. Update: create a revision on *latest*, or pick a specific revision
# MAGIC
# MAGIC Review the summary + collapsible DDL, then **Submit to SqlDBM**.
# MAGIC
# MAGIC > Tip: set the notebook's "On widget change" behavior to **Do nothing** so picking a value
# MAGIC > doesn't trigger a full rerun. Requires `ipywidgets` (available on current DBR / serverless).

# COMMAND ----------
# MAGIC %md ## Cell 1 — Discover catalogs

# COMMAND ----------
catalogs = sorted(r[0] for r in spark.sql("SHOW CATALOGS").collect())
if not catalogs:
    raise RuntimeError("No catalogs returned. Check Unity Catalog access on this compute.")
try:
    default_catalog = spark.catalog.currentCatalog()
except Exception:
    default_catalog = catalogs[0]
if default_catalog not in catalogs:
    default_catalog = catalogs[0]
try:
    dbutils.widgets.remove("catalog")
except Exception:
    pass
dbutils.widgets.dropdown("catalog", default_catalog, catalogs, "1. Catalog")
print(f"Found {len(catalogs)} catalog(s). Pick one in the 'Catalog' widget, then run Cell 2.")

# COMMAND ----------
# MAGIC %md ## Cell 2 — List schemas in the chosen catalog

# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
schemas = sorted(r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect())
if not schemas:
    raise RuntimeError(f"No schemas found in catalog '{catalog}'.")
try:
    dbutils.widgets.remove("schemas")
except Exception:
    pass
dbutils.widgets.multiselect("schemas", schemas[0], schemas, "2. Schemas")
print(f"Catalog '{catalog}' has {len(schemas)} schema(s). Select schema(s), then run Cell 3.")

# COMMAND ----------
# MAGIC %md ## Cell 3 — Generate DDL

# COMMAND ----------
catalog = dbutils.widgets.get("catalog")
selected_schemas = [s for s in dbutils.widgets.get("schemas").split(",") if s]

results = []     # [{catalog, schema, table, ddl}]
skipped = []     # [(schema, table_or_None, reason)]

for schema in selected_schemas:
    try:
        table_rows = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
    except Exception as e:
        skipped.append((schema, None, str(e).splitlines()[0]))
        continue
    for row in table_rows:
        d = row.asDict()
        table_name = d.get("tableName") or d.get("table") or row[1]
        if d.get("isTemporary"):
            continue
        fqtn = f"`{catalog}`.`{schema}`.`{table_name}`"
        try:
            ddl = spark.sql(f"SHOW CREATE TABLE {fqtn}").first()[0]
            results.append({"catalog": catalog, "schema": schema, "table": table_name, "ddl": ddl})
        except Exception as e:
            skipped.append((schema, table_name, str(e).splitlines()[0]))

combined_ddl = "\n".join(
    f"-- {r['catalog']}.{r['schema']}.{r['table']}\n{r['ddl']};\n" for r in results
)
print(f"Generated DDL for {len(results)} table(s) across {len(selected_schemas)} schema(s). "
      f"{len(skipped)} skipped/failed.")
for sch, tbl, reason in skipped[:50]:
    print(f"  - skipped {sch}.{tbl}: {reason[:120]}")
print("\nReview + push to SqlDBM in the form below (Cell 5).")

# COMMAND ----------
# MAGIC %md ## Cell 4 — SqlDBM API client

# COMMAND ----------
import json, time, html, requests
from datetime import datetime

SQLDBM_BASE = "https://api.sqldbm.com"   # production

# dbType values accepted by SqlDBM (case-insensitive). Databricks DDL -> "databricks".
DB_TYPES = ["databricks", "snowflake", "sqlServer", "postgreSQL", "redshift",
            "azureSynapse", "bigQuery", "oracle", "mySQL", "alloyDB", "logical"]

# Map create-time dbType (enum casing) -> URL segment used by app.sqldbm.com links.
# Falls back to the canonical dbType returned by the API when available.
URL_DBTYPE = {"databricks": "Databricks", "snowflake": "Snowflake", "sqlServer": "SQLServer",
              "postgreSQL": "PostgreSQL", "redshift": "Redshift", "azureSynapse": "AzureSynapse",
              "bigQuery": "BigQuery", "oracle": "Oracle", "mySQL": "MySQL",
              "alloyDB": "AlloyDB", "logical": "Logical"}

def _headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _get(path, token):
    r = requests.get(f"{SQLDBM_BASE}{path}", headers=_headers(token), timeout=60)
    r.raise_for_status()
    return r.json()

def list_projects(token):
    """-> list of {'id','name'}."""
    return _get("/projects", token).get("data", []) or []

def list_branches(token, project_id):
    """-> list of branch dicts (branchId, branchName, isMain). [] if none / not CW."""
    try:
        return (_get(f"/projects/{project_id}/branches", token).get("data", {}) or {}).get("branches", []) or []
    except requests.HTTPError:
        return []

def list_revisions(token, project_id, branch_id=None):
    """-> list of revision dicts. Best-effort; shape-tolerant."""
    base = f"/projects/{project_id}" + (f"/branches/{branch_id}" if branch_id else "")
    try:
        data = _get(f"{base}/revisions", token).get("data", [])
    except requests.HTTPError:
        return []
    if isinstance(data, dict):
        data = data.get("revisions", []) or []
    return data or []

def _diagram_block(diagram_name):
    return [{"subjectArea": None, "diagramName": diagram_name}] if diagram_name else None

def create_project(token, project_name, ddl, db_type, revision_name, diagram_name=None):
    body = {"dbType": db_type, "projectName": project_name, "sourceFormat": "ddl",
            "payload": ddl, "revisionName": revision_name}
    dg = _diagram_block(diagram_name)
    if dg:
        body["addToDiagram"] = dg
    return requests.post(f"{SQLDBM_BASE}/projects", headers=_headers(token),
                         data=json.dumps(body), timeout=120)

def create_revision(token, project_id, ddl, revision_name, strict=False,
                    branch_id=None, revision_id=None, diagram_name=None):
    """POST a revision. Routes through /branches/{branchId} when branch_id is set,
    and targets /revisions/{revisionId} when revision_id is set, else /revisions/last."""
    base = f"/projects/{project_id}" + (f"/branches/{branch_id}" if branch_id else "")
    target = f"/revisions/{revision_id}" if revision_id else "/revisions/last"
    body = {"sourceFormat": "ddl", "payload": ddl, "strictMode": strict, "revisionName": revision_name}
    dg = _diagram_block(diagram_name)
    if dg:
        body["addToDiagram"] = dg
    return requests.post(f"{SQLDBM_BASE}{base}{target}", headers=_headers(token),
                         data=json.dumps(body), timeout=120)

def create_branch(token, project_id, branch_name, revision_name="Initial branch revision"):
    return requests.post(f"{SQLDBM_BASE}/projects/{project_id}/branches", headers=_headers(token),
                         data=json.dumps({"branchName": branch_name, "revisionName": revision_name}),
                         timeout=120)

def wait_for_branch(token, project_id, branch_name, tries=10, delay=1.5):
    """Branch creation is async (202). Poll the branch list until it appears; return branchId or None."""
    for _ in range(tries):
        for b in list_branches(token, project_id):
            if b.get("branchName") == branch_name:
                return b.get("branchId")
        time.sleep(delay)
    return None

def wait_for_project(token, project_name, tries=10, delay=1.5):
    """Project creation is async (202) and returns no id. Poll the project list to resolve it."""
    target = project_name.strip().lower()
    for _ in range(tries):
        for p in list_projects(token):
            if str(p.get("name", "")).strip().lower() == target:
                return p.get("id")
        time.sleep(delay)
    return None

def get_project_dbtype(token, project_id):
    """Canonical dbType (e.g. 'SQLServer', 'Logical') from the latest revision, for building links."""
    try:
        info = (_get(f"/projects/{project_id}/revisions/last", token).get("data", {}) or {}).get("projectInfo", {}) or {}
        return info.get("dbType")
    except Exception:
        return None

# Branch deep-link. A branch is addressed by its own id in the same p<id> slot as a project,
# so the branch URL is the project URL pattern with branch_id in place of the project id.
BRANCH_URL_TEMPLATE = "https://app.sqldbm.com/{seg}/DatabaseExplorer/p{branch_id}/"

def project_link(db_type_segment, project_id):
    return f"https://app.sqldbm.com/{db_type_segment}/DatabaseExplorer/p{project_id}/"

def branch_link(db_type_segment, project_id, branch_id):
    return BRANCH_URL_TEMPLATE.format(seg=db_type_segment, branch_id=branch_id)

# current-user + timestamp for default new-branch name
try:
    _user = spark.sql("SELECT current_user() AS u").first()["u"]
except Exception:
    import getpass
    _user = getpass.getuser()
_user_slug = (_user or "user").split("@")[0].replace(".", "-")
RUN_TS = datetime.now().strftime("%Y%m%d-%H%M%S")
DEFAULT_BRANCH_NAME = f"databricks-import/{_user_slug}-{RUN_TS}"
print(f"API client ready. Default new-branch name: {DEFAULT_BRANCH_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 5 — Destination form: review & submit
# MAGIC Enter your token, click **Connect**, make your choices, review, then **Submit to SqlDBM**.

# COMMAND ----------
import ipywidgets as widgets
from IPython.display import display, clear_output, HTML

NEW_PROJECT = "➕  Create new project"
NEW_BRANCH = "➕  Create new branch"
LATEST = "Create revision on latest"

_state = {"projects": {}, "branch_ids": {}, "revision_ids": {}}

# --- controls ---
token_w        = widgets.Password(description="API Token", layout=widgets.Layout(width="420px"),
                                  style={"description_width": "120px"})
connect_btn    = widgets.Button(description="Connect & load projects", button_style="info",
                                layout=widgets.Layout(width="220px"))
project_dd     = widgets.Dropdown(options=[], description="Project", disabled=True,
                                  layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
new_proj_name  = widgets.Text(description="New name", placeholder="Unique project name",
                              layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
db_type_dd     = widgets.Dropdown(options=DB_TYPES, value="databricks", description="dbType",
                                  layout=widgets.Layout(width="300px"), style={"description_width": "120px"})
cw_chk         = widgets.Checkbox(value=False, description="Concurrent-Working project (route via a branch)",
                                  indent=False)
branch_dd      = widgets.Dropdown(options=[], description="Branch",
                                  layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
new_branch_w   = widgets.Text(value=DEFAULT_BRANCH_NAME, description="Branch name",
                              layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
update_dd      = widgets.Dropdown(options=[LATEST], value=LATEST, description="Update",
                                  layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
revision_name_w= widgets.Text(value=f"Databricks import {RUN_TS}", description="Revision name",
                              layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
diagram_w      = widgets.Text(value="", description="Diagram", placeholder="(optional) place objects on a diagram",
                              layout=widgets.Layout(width="420px"), style={"description_width": "120px"})
strict_w       = widgets.Checkbox(value=False, description="strictMode (override another user's lock)", indent=False)
submit_btn     = widgets.Button(description="Submit to SqlDBM", button_style="success", disabled=True,
                                layout=widgets.Layout(width="220px"))
status_out     = widgets.Output()
review_out     = widgets.Output()
result_out     = widgets.Output()

def _set(w, show):
    w.layout.display = "" if show else "none"

def name_conflict():
    """Return a warning string if the chosen new project/branch name collides with an existing one."""
    if project_dd.value == NEW_PROJECT:
        nm = new_proj_name.value.strip().lower()
        if nm and nm in _state.get("project_names", set()):
            return f"Project name '{new_proj_name.value.strip()}' already exists — names must be unique."
    elif project_dd.value and cw_chk.value and branch_dd.value == NEW_BRANCH:
        bn = new_branch_w.value.strip().lower()
        if bn and bn in _state.get("branch_names", set()):
            return f"Branch name '{new_branch_w.value.strip()}' already exists on this project — choose a unique name."
    return ""

def render_review(*_):
    review_out.clear_output()
    with review_out:
        n = len(results)
        dest = "(choose a project)"
        if project_dd.value == NEW_PROJECT:
            dest = f"NEW project '{new_proj_name.value or '...'}' (dbType={db_type_dd.value})"
        elif project_dd.value:
            dest = f"{project_dd.value}"
            if cw_chk.value:
                b = new_branch_w.value if branch_dd.value == NEW_BRANCH else branch_dd.value
                dest += f"  ·  branch: {b}"
            dest += f"  ·  {update_dd.value}"
        warn = name_conflict()
        warn_html = f"<div style='color:#c00;margin-top:4px'>⚠ {html.escape(warn)}</div>" if warn else ""
        display(HTML(
            "<div style='font-family:sans-serif;font-size:13px'>"
            f"<b>Source:</b> {html.escape(catalog)} · schema(s): {html.escape(', '.join(selected_schemas))} "
            f"· <b>{n}</b> table(s)<br>"
            f"<b>Destination:</b> {html.escape(dest)}<br>"
            f"<b>Revision name:</b> {html.escape(revision_name_w.value)}"
            f"{' · strictMode' if strict_w.value else ''}"
            f"{(' · diagram: ' + html.escape(diagram_w.value)) if diagram_w.value.strip() else ''}"
            f"{warn_html}"
            "<details style='margin-top:8px'>"
            f"<summary style='cursor:pointer'>Show full DDL ({n} table(s))</summary>"
            "<div style='max-height:480px;overflow:auto;border:1px solid #ccc;padding:8px;"
            "font-family:monospace;white-space:pre;font-size:12px;margin-top:6px'>"
            f"{html.escape(combined_ddl) or '(no DDL — run Cell 3)'}</div></details>"
            "</div>"))
    submit_btn.disabled = (not bool(project_dd.value)) or bool(name_conflict())

def refresh_conditional_fields():
    is_new = project_dd.value == NEW_PROJECT
    _set(new_proj_name, is_new)
    _set(db_type_dd, is_new)
    _set(cw_chk, not is_new and bool(project_dd.value))
    show_branch = (not is_new) and cw_chk.value
    _set(branch_dd, show_branch)
    _set(new_branch_w, show_branch and branch_dd.value == NEW_BRANCH)
    _set(update_dd, (not is_new) and bool(project_dd.value))
    submit_btn.disabled = not bool(project_dd.value)

def load_project_context(project_id):
    token = token_w.value.strip()
    branches = list_branches(token, project_id)
    is_cw = len(branches) > 1 or any(not b.get("isMain", True) for b in branches)
    cw_chk.value = is_cw
    _state["branch_ids"] = {}
    _state["branch_names"] = {str(b.get("branchName", "")).strip().lower() for b in branches}
    labels = []
    for b in sorted(branches, key=lambda x: str(x.get("branchName", "")).lower()):
        label = b.get("branchName", "?") + ("  (main)" if b.get("isMain") else "")
        _state["branch_ids"][label] = b.get("branchId")
        labels.append(label)
    branch_dd.options = [NEW_BRANCH] + labels
    branch_dd.value = next((o for o in labels if "(main)" not in o), NEW_BRANCH)
    # revisions for the "update" dropdown: latest on top, then specific revisions descending
    _state["revision_ids"] = {}
    revs = []
    for rv in list_revisions(token, project_id):
        rid = rv.get("revisionId") or rv.get("id")
        if rid is None:
            continue
        num = rv.get("revNumber") or rv.get("number") or rid
        nm = rv.get("revName") or rv.get("name") or ""
        revs.append((num, rid, nm))
    rev_opts = [LATEST]
    for num, rid, nm in sorted(revs, key=lambda t: (t[0] if isinstance(t[0], int) else 0), reverse=True):
        label = f"From revision {num}: {nm}".strip()
        _state["revision_ids"][label] = rid
        rev_opts.append(label)
    update_dd.options = rev_opts
    update_dd.value = LATEST

def on_connect(_):
    status_out.clear_output()
    with status_out:
        token = token_w.value.strip()
        if not token:
            print("Enter your API token first.")
            return
        try:
            projects = list_projects(token)
        except Exception as e:
            print(f"Could not load projects: {e}")
            return
        projects_sorted = sorted(projects, key=lambda p: str(p.get("name", "")).lower())
        _state["projects"] = {f"{p['name']}  (#{p['id']})": p["id"] for p in projects_sorted}
        _state["project_names"] = {str(p.get("name", "")).strip().lower() for p in projects}
        project_dd.options = [NEW_PROJECT] + list(_state["projects"].keys())
        project_dd.value = NEW_PROJECT
        project_dd.disabled = False
        print(f"Connected. {len(projects)} existing project(s) loaded.")
    refresh_conditional_fields()
    render_review()

def on_project_change(_):
    if project_dd.value and project_dd.value != NEW_PROJECT:
        with status_out:
            try:
                load_project_context(_state["projects"][project_dd.value])
            except Exception as e:
                print(f"Could not load project context: {e}")
    refresh_conditional_fields()
    render_review()

def on_submit(_):
    result_out.clear_output()
    with result_out:
        token = token_w.value.strip()
        if not token:
            print("Enter your API token."); return
        if not combined_ddl:
            print("No DDL to send — run Cell 3 first."); return
        conflict = name_conflict()
        if conflict:
            print(f"⛔ {conflict}"); return
        rev_name = revision_name_w.value.strip() or f"Databricks import {RUN_TS}"
        diagram = diagram_w.value.strip() or None
        is_new = project_dd.value == NEW_PROJECT
        created_name = None
        pid = None
        try:
            if is_new:
                created_name = new_proj_name.value.strip()
                if not created_name:
                    print("Enter a name for the new project."); return
                print(f"Creating new project '{created_name}' ...")
                r = create_project(token, created_name, combined_ddl, db_type_dd.value, rev_name, diagram)
            else:
                pid = _state["projects"][project_dd.value]
                branch_id = None
                branch_name = None
                if cw_chk.value:
                    if branch_dd.value == NEW_BRANCH:
                        branch_name = new_branch_w.value.strip() or DEFAULT_BRANCH_NAME
                        print(f"Creating branch '{branch_name}' ...")
                        cr = create_branch(token, pid, branch_name, rev_name)
                        if cr.status_code not in (200, 202):
                            print(f"❌ Branch create failed {cr.status_code}: {cr.text}"); return
                        branch_id = wait_for_branch(token, pid, branch_name)
                        if not branch_id:
                            print("Branch was accepted but isn't visible yet. Re-select it in a moment and submit."); return
                    else:
                        branch_id = _state["branch_ids"].get(branch_dd.value)
                        branch_name = branch_dd.value.replace("  (main)", "")
                rev_id = None if update_dd.value == LATEST else _state["revision_ids"].get(update_dd.value)
                where = f"branch {branch_id}" if branch_id else "main"
                tgt = f"revision {rev_id}" if rev_id else "latest"
                print(f"Creating revision on {where} ({tgt}) ...")
                r = create_revision(token, pid, combined_ddl, rev_name, strict_w.value, branch_id, rev_id, diagram)

            if r.status_code in (200, 202):
                print(f"✅ {r.status_code} — accepted. SqlDBM is processing the import.")
                try:
                    if is_new:
                        new_id = wait_for_project(token, created_name)
                        if new_id:
                            seg = get_project_dbtype(token, new_id) or URL_DBTYPE.get(db_type_dd.value, db_type_dd.value)
                            _show_links(seg, new_id)
                        else:
                            print("New project accepted but not queryable yet — open SqlDBM to find it shortly.")
                    else:
                        seg = get_project_dbtype(token, pid)
                        if seg:
                            _show_links(seg, pid, branch_id, branch_name)
                        else:
                            print("Submitted OK; open SqlDBM to view the project "
                                  "(couldn't resolve dbType to build a link).")
                except Exception as e:
                    print(f"(submitted OK; couldn't build link: {e})")
            else:
                print(f"❌ {r.status_code}: {r.text}")
        except Exception as e:
            print(f"Error: {e}")

def _link_html(label, url):
    return (f"<div style='margin-top:6px'>🔗 <b>{html.escape(label)}:</b> "
            f"<a href='{html.escape(url)}' target='_blank'>{html.escape(url)}</a></div>")

def _show_links(seg, project_id, branch_id=None, branch_name=None):
    # Always show the main/project link; for a branch import, also show the branch link.
    display(HTML(_link_html("Main branch", project_link(seg, project_id))))
    if branch_id is not None:
        label = f"Branch '{branch_name}'" if branch_name else "Branch"
        display(HTML(_link_html(label, branch_link(seg, project_id, branch_id))))

# wiring
connect_btn.on_click(on_connect)
submit_btn.on_click(on_submit)
project_dd.observe(on_project_change, names="value")
cw_chk.observe(lambda c: (refresh_conditional_fields(), render_review()), names="value")
branch_dd.observe(lambda c: (refresh_conditional_fields(), render_review()), names="value")
for w in (new_proj_name, db_type_dd, update_dd, revision_name_w, diagram_w, strict_w, new_branch_w):
    w.observe(render_review, names="value")

refresh_conditional_fields()
render_review()

display(widgets.VBox([
    widgets.HTML("<h4 style='margin:4px 0'>Configure your Destination Project</h4>"),
    widgets.HBox([token_w, connect_btn]),
    status_out,
    project_dd, new_proj_name, db_type_dd,
    cw_chk, branch_dd, new_branch_w,
    update_dd, revision_name_w, diagram_w, strict_w,
    widgets.HTML("<hr style='margin:8px 0'>"),
    review_out,
    submit_btn,
    result_out,
]))
