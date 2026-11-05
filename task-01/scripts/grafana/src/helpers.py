import copy

def throughput_query(endpoint, method):
    query = f"sum(rate(requests_total{{endpoint='{endpoint}', method='{method}', datacenter=~'$datacenter', pod_id=~'$pod_id'}}[1m]))"
    return query

def errors_query(endpoint, method):
    query = f"sum by (status_code) (rate(errors_total{{endpoint='{endpoint}', method='{method}', datacenter=~'$datacenter', pod_id=~'$pod_id'}}[1m]))"
    return query

def latency_p50_query(endpoint, method):
    query = f"histogram_quantile(0.50, sum by (le) (rate(request_duration_seconds_bucket{{endpoint='{endpoint}', method='{method}', datacenter=~'$datacenter', pod_id=~'$pod_id', status_code=~'2..'}}[1m])))"
    return query

def latency_p95_query(endpoint, method):
    query = f"histogram_quantile(0.95, sum by (le) (rate(request_duration_seconds_bucket{{endpoint='{endpoint}', method='{method}', datacenter=~'$datacenter', pod_id=~'$pod_id', status_code=~'2..'}}[1m])))"
    return query

def latency_p99_query(endpoint, method):
    query = f"histogram_quantile(0.99, sum by (le) (rate(request_duration_seconds_bucket{{endpoint='{endpoint}', method='{method}', datacenter=~'$datacenter', pod_id=~'$pod_id', status_code=~'2..'}}[1m])))"
    return query

def create_dashboard(dashboard_template, handlers):
    dashboard = copy.deepcopy(dashboard_template)

    panel_template_throughput = copy.deepcopy(dashboard["spec"]["elements"]["panel-1"])
    panel_template_errors = copy.deepcopy(dashboard["spec"]["elements"]["panel-2"])
    panel_template_latency = copy.deepcopy(dashboard["spec"]["elements"]["panel-3"])

    row_template = copy.deepcopy(dashboard["spec"]["layout"]["spec"]["rows"][0])

    elements = {}
    rows = []
    panel_id = 1

    for handler in handlers:
        endpoint = handler["endpoint"]
        method = handler["method"][0]

        panel_name_throughput = f"panel-{panel_id}"
        panel_name_errors = f"panel-{panel_id + 1}"
        panel_name_latency = f"panel-{panel_id + 2}"
        panel_id += 3

        p1 = copy.deepcopy(panel_template_throughput)
        p2 = copy.deepcopy(panel_template_errors)
        p3 = copy.deepcopy(panel_template_latency)

        p1["spec"]["data"]["spec"]["queries"][0]["spec"]["query"]["spec"]["expr"] = throughput_query(endpoint, method)
        p2["spec"]["data"]["spec"]["queries"][0]["spec"]["query"]["spec"]["expr"] = errors_query(endpoint, method)

        p3["spec"]["data"]["spec"]["queries"][0]["spec"]["query"]["spec"]["expr"] = latency_p50_query(endpoint, method)
        p3["spec"]["data"]["spec"]["queries"][1]["spec"]["query"]["spec"]["expr"] = latency_p95_query(endpoint, method)
        p3["spec"]["data"]["spec"]["queries"][2]["spec"]["query"]["spec"]["expr"] = latency_p99_query(endpoint, method)

        p1["spec"]["id"] = panel_id - 3
        p2["spec"]["id"] = panel_id - 2
        p3["spec"]["id"] = panel_id - 1

        p1["spec"]["title"] = "01 | Throughput"
        p2["spec"]["title"] = "02 | Errors"
        p3["spec"]["title"] = "03 | Latency (2xx)"

        elements[panel_name_throughput] = p1
        elements[panel_name_errors] = p2
        elements[panel_name_latency] = p3

        row = copy.deepcopy(row_template)
        row["spec"]["title"] = f"{method} {endpoint}"
        items = row["spec"]["layout"]["spec"]["items"]

        panel_names = [panel_name_throughput, panel_name_errors, panel_name_latency]

        for item, name in zip(items, panel_names):
            item["spec"]["element"]["name"] = name

        rows.append(row)

    dashboard["spec"]["elements"] = elements
    dashboard["spec"]["layout"]["spec"]["rows"] = rows

    return dashboard
