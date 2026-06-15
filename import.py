"""
Databricks -> SqlDBM DDL importer  (single-cell interactive app).

Run from a Databricks notebook cell:

    import urllib.request
    url = "https://raw.githubusercontent.com/sqldbmcorp/poc-databricks-re-notebook/refs/heads/main/import.py"
    exec(compile(urllib.request.urlopen(url).read().decode(), "import.py", "exec"), globals())

Renders one form: pick a source catalog + schema(s), Generate DDL, then configure the
SqlDBM destination (token, project/branch/revision), review, and Submit.

Requirements: ipywidgets (current DBR / serverless), Unity Catalog read access, and outbound
HTTPS to api.sqldbm.com. `spark` is taken from the calling notebook's globals.
"""

import json, time, html, requests
from datetime import datetime
import ipywidgets as widgets
from IPython.display import display, HTML

# spark comes from the calling notebook; fall back to a session if exec'd elsewhere
try:
    spark  # noqa: F821
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

# ============================================================ config
SQLDBM_BASE = "https://api.sqldbm.com"   # production
DB_TYPES = ["databricks", "snowflake", "sqlServer", "postgreSQL", "redshift",
            "azureSynapse", "bigQuery", "oracle", "mySQL", "alloyDB", "logical"]
URL_DBTYPE = {"databricks": "Databricks", "snowflake": "Snowflake", "sqlServer": "SQLServer",
              "postgreSQL": "PostgreSQL", "redshift": "Redshift", "azureSynapse": "AzureSynapse",
              "bigQuery": "BigQuery", "oracle": "Oracle", "mySQL": "MySQL",
              "alloyDB": "AlloyDB", "logical": "Logical"}
# A branch is addressed by its own id in the same p<id> slot as a project.
BRANCH_URL_TEMPLATE = "https://app.sqldbm.com/{seg}/DatabaseExplorer/p{branch_id}/"

# ============================================================ SqlDBM API client
def _headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _get(path, token):
    r = requests.get(f"{SQLDBM_BASE}{path}", headers=_headers(token), timeout=60)
    r.raise_for_status()
    return r.json()

def list_projects(token):
    return _get("/projects", token).get("data", []) or []

def list_branches(token, project_id):
    try:
        return (_get(f"/projects/{project_id}/branches", token).get("data", {}) or {}).get("branches", []) or []
    except requests.HTTPError:
        return []

def list_revisions(token, project_id, branch_id=None):
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
    for _ in range(tries):
        for b in list_branches(token, project_id):
            if b.get("branchName") == branch_name:
                return b.get("branchId")
        time.sleep(delay)
    return None

def wait_for_project(token, project_name, tries=10, delay=1.5):
    target = project_name.strip().lower()
    for _ in range(tries):
        for p in list_projects(token):
            if str(p.get("name", "")).strip().lower() == target:
                return p.get("id")
        time.sleep(delay)
    return None

def get_project_dbtype(token, project_id):
    try:
        info = (_get(f"/projects/{project_id}/revisions/last", token).get("data", {}) or {}).get("projectInfo", {}) or {}
        return info.get("dbType")
    except Exception:
        return None

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

# ============================================================ shared state (source side)
results = []            # every object generated from the chosen schemas
selected_results = []   # subset the user kept checked in Step 2
combined_ddl = ""       # DDL payload built from selected_results only
catalog = ""
selected_schemas = []
_object_rows = []       # [(checkbox, result_dict)]
_bulk = {"active": False}

NEW_PROJECT = "➕  Create new project"
NEW_BRANCH = "➕  Create new branch"
LATEST = "Create revision on latest"
_state = {"projects": {}, "branch_ids": {}, "revision_ids": {}}
_W = {"width": "420px"}
_S = {"description_width": "120px"}

# ============================================================ SOURCE controls
def _list_catalogs():
    return sorted(r[0] for r in spark.sql("SHOW CATALOGS").collect())

def _list_schemas(cat):
    return sorted(r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect())

catalog_dd   = widgets.Dropdown(description="Catalog", options=[],
                                layout=widgets.Layout(**_W), style=_S)
schema_sel   = widgets.SelectMultiple(description="Schema(s)", options=[], rows=8,
                                      layout=widgets.Layout(**_W), style=_S)
generate_btn = widgets.Button(description="Generate DDL", button_style="primary",
                              layout=widgets.Layout(width="220px"))
source_out   = widgets.Output()

# ============================================================ STEP 2 controls (object picker)
objects_summary  = widgets.HTML("Generate DDL in Step 1 to list objects.")
objects_container= widgets.VBox([])
select_all_btn   = widgets.Button(description="Select all", layout=widgets.Layout(width="110px"))
select_none_btn  = widgets.Button(description="Deselect all", layout=widgets.Layout(width="110px"))

def on_catalog_change(_=None):
    try:
        schema_sel.options = _list_schemas(catalog_dd.value)
    except Exception as e:
        with source_out:
            print(f"Could not list schemas for '{catalog_dd.value}': {e}")

def on_generate(_):
    global results, catalog, selected_schemas
    source_out.clear_output()
    catalog = catalog_dd.value
    selected_schemas = list(schema_sel.value)
    with source_out:
        if not selected_schemas:
            print("Select at least one schema, then click Generate DDL.")
            return
        res, skipped = [], []
        for schema in selected_schemas:
            try:
                rows = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
            except Exception as e:
                skipped.append((schema, None, str(e).splitlines()[0]))
                continue
            for row in rows:
                d = row.asDict()
                t = d.get("tableName") or d.get("table") or row[1]
                if d.get("isTemporary"):
                    continue
                try:
                    ddl = spark.sql(f"SHOW CREATE TABLE `{catalog}`.`{schema}`.`{t}`").first()[0]
                    res.append({"catalog": catalog, "schema": schema, "table": t, "ddl": ddl})
                except Exception as e:
                    skipped.append((schema, t, str(e).splitlines()[0]))
        results = res
        print(f"Found {len(res)} object(s) across {len(selected_schemas)} schema(s). "
              f"{len(skipped)} skipped/failed. Review and (de)select them in Step 2.")
        for s, t, reason in skipped[:25]:
            print(f"  - skipped {s}.{t}: {reason[:120]}")
    build_object_rows()
    recompute_selection()

def _object_kind(ddl):
    head = (" " + ddl[:80].upper() + " ")
    if " VIEW " in head:
        return "VIEW"
    if " FUNCTION " in head:
        return "FUNCTION"
    if " PROCEDURE " in head:
        return "PROCEDURE"
    return "TABLE"

def build_object_rows():
    """Render one selectable row per generated object, grouped by schema, DDL behind a caret."""
    global _object_rows
    _object_rows = []
    if not results:
        objects_container.children = []
        return
    by_schema = {}
    for r in results:
        by_schema.setdefault(r["schema"], []).append(r)
    children = []
    for schema in sorted(by_schema):
        children.append(widgets.HTML(
            f"<div style='font-weight:600;margin:8px 0 2px'>{html.escape(catalog)}.{html.escape(schema)}</div>"))
        for r in sorted(by_schema[schema], key=lambda x: x["table"].lower()):
            chk = widgets.Checkbox(value=True, indent=False,
                                   description=f"{r['table']}   ·   {_object_kind(r['ddl'])}",
                                   layout=widgets.Layout(width="380px", margin="0"))
            chk.observe(lambda c: recompute_selection(), names="value")
            details = widgets.HTML(
                "<details style='margin:0 0 4px 26px'>"
                "<summary style='cursor:pointer;font-size:12px;color:#555'>show DDL</summary>"
                "<div style='max-height:300px;overflow:auto;border:1px solid #ddd;padding:6px;"
                "font-family:monospace;white-space:pre;font-size:12px;margin-top:4px'>"
                f"{html.escape(r['ddl'])}</div></details>")
            _object_rows.append((chk, r))
            children.append(widgets.VBox([chk, details], layout=widgets.Layout(margin="0")))
    objects_container.children = children

def recompute_selection(*_):
    """Rebuild the submission payload from the currently checked objects."""
    global combined_ddl, selected_results
    if _bulk["active"]:
        return
    selected_results = [r for chk, r in _object_rows if chk.value]
    combined_ddl = "\n".join(
        f"-- {r['catalog']}.{r['schema']}.{r['table']}\n{r['ddl']};\n" for r in selected_results)
    total = len(_object_rows)
    objects_summary.value = (f"<b>{len(selected_results)}</b> of {total} object(s) selected"
                             if total else "Generate DDL in Step 1 to list objects.")
    render_review()

def set_all(value):
    _bulk["active"] = True
    for chk, _ in _object_rows:
        chk.value = value
    _bulk["active"] = False
    recompute_selection()

# ============================================================ DESTINATION controls
token_w        = widgets.Password(description="API Token", layout=widgets.Layout(**_W), style=_S)
connect_btn    = widgets.Button(description="Connect & load projects", button_style="info",
                                layout=widgets.Layout(width="220px"))
project_dd     = widgets.Dropdown(options=[], description="Project", disabled=True,
                                  layout=widgets.Layout(**_W), style=_S)
new_proj_name  = widgets.Text(description="New name", placeholder="Unique project name",
                              layout=widgets.Layout(**_W), style=_S)
db_type_dd     = widgets.Dropdown(options=DB_TYPES, value="databricks", description="dbType",
                                  layout=widgets.Layout(width="300px"), style=_S)
cw_chk         = widgets.Checkbox(value=False, indent=False,
                                  description="Concurrent-Working project (route via a branch)")
branch_dd      = widgets.Dropdown(options=[], description="Branch",
                                  layout=widgets.Layout(**_W), style=_S)
new_branch_w   = widgets.Text(value=DEFAULT_BRANCH_NAME, description="Branch name",
                              layout=widgets.Layout(**_W), style=_S)
update_dd      = widgets.Dropdown(options=[LATEST], value=LATEST, description="Update",
                                  layout=widgets.Layout(**_W), style=_S)
revision_name_w= widgets.Text(value=f"Databricks import {RUN_TS}", description="Revision name",
                              layout=widgets.Layout(**_W), style=_S)
diagram_w      = widgets.Text(value="", description="Diagram",
                              placeholder="(optional) place objects on a diagram",
                              layout=widgets.Layout(**_W), style=_S)
strict_w       = widgets.Checkbox(value=False, indent=False,
                                  description="strictMode (override another user's lock)")
submit_btn     = widgets.Button(description="Submit to SqlDBM", button_style="success",
                                disabled=True, layout=widgets.Layout(width="220px"))
status_out     = widgets.Output()
review_out     = widgets.Output()
result_out     = widgets.Output()

def _set(w, show):
    w.layout.display = "" if show else "none"

def name_conflict():
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
        n = len(selected_results)
        total = len(results)
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
        src = f"{html.escape(catalog)} · schema(s): {html.escape(', '.join(selected_schemas))}" if catalog else "(generate DDL first)"
        display(HTML(
            "<div style='font-family:sans-serif;font-size:13px'>"
            f"<b>Source:</b> {src} · <b>{n}</b> of {total} object(s) selected<br>"
            f"<b>Destination:</b> {html.escape(dest)}<br>"
            f"<b>Revision name:</b> {html.escape(revision_name_w.value)}"
            f"{' · strictMode' if strict_w.value else ''}"
            f"{(' · diagram: ' + html.escape(diagram_w.value)) if diagram_w.value.strip() else ''}"
            f"{warn_html}"
            "<details style='margin-top:8px'>"
            f"<summary style='cursor:pointer'>Show DDL to be submitted ({n} object(s))</summary>"
            "<div style='max-height:480px;overflow:auto;border:1px solid #ccc;padding:8px;"
            "font-family:monospace;white-space:pre;font-size:12px;margin-top:6px'>"
            f"{html.escape(combined_ddl) or '(no DDL — click Generate DDL)'}</div></details>"
            "</div>"))
    submit_btn.disabled = (not bool(project_dd.value)) or bool(name_conflict()) or (not combined_ddl)

def refresh_conditional_fields():
    is_new = project_dd.value == NEW_PROJECT
    _set(new_proj_name, is_new)
    _set(db_type_dd, is_new)
    _set(cw_chk, not is_new and bool(project_dd.value))
    show_branch = (not is_new) and cw_chk.value
    _set(branch_dd, show_branch)
    _set(new_branch_w, show_branch and branch_dd.value == NEW_BRANCH)
    _set(update_dd, (not is_new) and bool(project_dd.value))

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
            print("No DDL to send — pick schema(s) and click Generate DDL first."); return
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
    display(HTML(_link_html("Main branch", project_link(seg, project_id))))
    if branch_id is not None:
        label = f"Branch '{branch_name}'" if branch_name else "Branch"
        display(HTML(_link_html(label, branch_link(seg, project_id, branch_id))))

# ============================================================ wire up
catalog_dd.observe(on_catalog_change, names="value")
generate_btn.on_click(on_generate)
select_all_btn.on_click(lambda b: set_all(True))
select_none_btn.on_click(lambda b: set_all(False))
connect_btn.on_click(on_connect)
submit_btn.on_click(on_submit)
project_dd.observe(on_project_change, names="value")
cw_chk.observe(lambda c: (refresh_conditional_fields(), render_review()), names="value")
branch_dd.observe(lambda c: (refresh_conditional_fields(), render_review()), names="value")
for _w in (new_proj_name, db_type_dd, update_dd, revision_name_w, diagram_w, strict_w, new_branch_w):
    _w.observe(render_review, names="value")

# ============================================================ initialize + render
catalog_dd.options = _list_catalogs()
try:
    _cur = spark.catalog.currentCatalog()
    if _cur in catalog_dd.options:
        catalog_dd.value = _cur
except Exception:
    pass
on_catalog_change()
refresh_conditional_fields()
render_review()

display(widgets.VBox([
    widgets.HTML("<h4 style='margin:4px 0'>1 · Configure Source Catalog and Schema(s)</h4>"),
    catalog_dd, schema_sel, generate_btn, source_out,
    widgets.HTML("<hr style='margin:10px 0'>"),
    widgets.HTML("<h4 style='margin:4px 0'>2 · Configure Objects to Import</h4>"),
    widgets.HBox([select_all_btn, select_none_btn, objects_summary]),
    objects_container,
    widgets.HTML("<hr style='margin:10px 0'>"),
    widgets.HTML("<h4 style='margin:4px 0'>3 · Configure Destination Project</h4>"),
    widgets.HBox([token_w, connect_btn]), status_out,
    project_dd, new_proj_name, db_type_dd,
    cw_chk, branch_dd, new_branch_w,
    update_dd, revision_name_w, diagram_w, strict_w,
    widgets.HTML("<hr style='margin:8px 0'>"),
    review_out, submit_btn, result_out,
]))
