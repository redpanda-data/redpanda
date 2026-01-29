#!/usr/bin/env python3
"""
Convert LSM trace logs to interactive HTML timeline.

Usage:
    ./logs_to_trace.py < logfile.log > trace.html
    ./logs_to_trace.py logfile.log > trace.html

Open the output HTML file in any web browser.
"""

import json
import re
import sys
from datetime import datetime
from collections import defaultdict
import html


# Log format: LEVEL TIMESTAMP [shard N:CONTEXT] LOGGER - FILE:LINE - MESSAGE
LOG_PATTERN = re.compile(
    r"^(?P<level>\w+)\s+"
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+"
    r"\[shard (?P<shard>\d+):\w+\]\s+"
    r"lsm\s+-\s+"
    r"(?P<file>\w+\.cc):(?P<line>\d+)\s+-\s+"
    r"(?P<message>.+)$"
)


def parse_timestamp(ts_str: str) -> int:
    """Parse seastar log timestamp to microseconds since epoch."""
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
    return int(dt.timestamp() * 1_000_000)


def parse_attrs(message: str) -> dict[str, str]:
    """Parse key=value attributes from message, supporting quoted values."""
    attrs = {}
    # Match key=value or key="quoted value"
    for match in re.finditer(r'(\w+)=(?:"([^"]*)"|(\S+))', message):
        k = match.group(1)
        v = match.group(2) if match.group(2) is not None else match.group(3)
        try:
            attrs[k] = int(v)
        except ValueError:
            attrs[k] = v
    return attrs


def generate_color(name: str) -> str:
    """Generate a consistent color for each event name."""
    # Simple hash-based color generation
    h = hash(name) % 360
    return f"hsl({h}, 65%, 60%)"


def main():
    # Read input
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    # Track events per shard
    events_by_shard = defaultdict(list)
    pending_begins = defaultdict(
        lambda: defaultdict(list)
    )  # [shard][name] -> stack of begins
    min_time = float("inf")
    max_time = 0

    for line in lines:
        match = LOG_PATTERN.match(line.strip())
        if not match:
            continue

        msg = match.group("message")
        parts = msg.split()
        if not parts:
            continue

        op = parts[0]
        shard = int(match.group("shard"))
        timestamp = parse_timestamp(match.group("timestamp"))
        attrs = parse_attrs(msg)

        min_time = min(min_time, timestamp)
        max_time = max(max_time, timestamp)

        # Determine phase and create events
        if op.endswith("_start"):
            name = op[:-6]
            # Store begin event to match with end
            pending_begins[shard][name].append({"timestamp": timestamp, "attrs": attrs})
        elif op.endswith("_end"):
            name = op[:-4]
            # Match with begin event
            if pending_begins[shard][name]:
                begin = pending_begins[shard][name].pop()
                # Create span event
                event = {
                    "name": name,
                    "start": begin["timestamp"],
                    "end": timestamp,
                    "type": "span",
                    "attrs": {**begin["attrs"], **attrs},
                }
                events_by_shard[shard].append(event)
            else:
                # Unmatched end - create instant marker
                event = {
                    "name": name + "_end",
                    "start": timestamp,
                    "end": timestamp,
                    "type": "instant",
                    "attrs": attrs,
                }
                events_by_shard[shard].append(event)
        else:
            # Instant event
            event = {
                "name": op,
                "start": timestamp,
                "end": timestamp,
                "type": "instant",
                "attrs": attrs,
            }
            events_by_shard[shard].append(event)

    # Handle any unmatched begin events
    for shard, names_dict in pending_begins.items():
        for name, begins in names_dict.items():
            for begin in begins:
                event = {
                    "name": name + "_start",
                    "start": begin["timestamp"],
                    "end": begin["timestamp"],
                    "type": "instant",
                    "attrs": begin["attrs"],
                }
                events_by_shard[shard].append(event)

    if not events_by_shard or min_time == float("inf"):
        print("<html><body><h1>No trace events found in logs</h1></body></html>")
        return

    # Generate HTML
    time_span = max_time - min_time
    if time_span == 0:
        time_span = 1000000  # 1 second default

    # Convert events to JSON for JavaScript and collect all event names
    events_data = {}
    all_event_names = set()
    for shard in sorted(events_by_shard.keys()):
        events_data[shard] = events_by_shard[shard]
        for event in events_by_shard[shard]:
            all_event_names.add(event["name"])

    all_event_names = sorted(all_event_names)

    html_output = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>LSM Trace Timeline</title>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #1e1e1e;
            color: #d4d4d4;
        }}
        h1 {{
            margin: 0 0 20px 0;
            font-size: 24px;
        }}
        #controls {{
            margin-bottom: 10px;
            padding: 10px;
            background: #252526;
            border: 1px solid #3e3e3e;
            border-radius: 4px;
            display: flex;
            gap: 20px;
            align-items: center;
            flex-wrap: wrap;
        }}
        #shard-selector {{
            padding: 5px 10px;
            background: #3c3c3c;
            border: 1px solid #3e3e3e;
            color: #d4d4d4;
            border-radius: 3px;
            font-size: 13px;
        }}
        #filter-panel {{
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .filter-item {{
            display: flex;
            align-items: center;
            gap: 5px;
            cursor: pointer;
        }}
        .filter-checkbox {{
            width: 16px;
            height: 16px;
            cursor: pointer;
        }}
        .filter-label {{
            font-size: 12px;
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .color-box {{
            width: 12px;
            height: 12px;
            border-radius: 2px;
            display: inline-block;
        }}
        #time-info {{
            margin-left: auto;
            color: #9e9e9e;
            font-size: 12px;
        }}
        #container {{
            position: relative;
            border: 1px solid #3e3e3e;
            background: #252526;
            overflow: auto;
            height: calc(100vh - 180px);
        }}
        #timeline {{
            position: relative;
            min-width: 100%;
        }}
        .event-row {{
            position: relative;
            height: 40px;
            border-bottom: 1px solid #3e3e3e;
        }}
        .event-row.hidden {{
            display: none;
        }}
        .row-label {{
            position: sticky;
            left: 0;
            display: inline-block;
            width: 200px;
            padding: 10px;
            background: #252526;
            border-right: 1px solid #3e3e3e;
            z-index: 10;
            font-size: 12px;
            font-weight: 500;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        .row-content {{
            position: absolute;
            left: 200px;
            top: 0;
            right: 0;
            height: 100%;
        }}
        .event {{
            position: absolute;
            height: 30px;
            top: 5px;
            border-radius: 3px;
            cursor: pointer;
            transition: filter 0.1s;
            border: 1px solid rgba(0,0,0,0.3);
            box-sizing: border-box;
        }}
        .event:hover {{
            filter: brightness(1.3);
            z-index: 5;
        }}
        .event.instant {{
            width: 3px !important;
            border-radius: 0;
        }}
        .event-label {{
            position: absolute;
            left: 5px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 11px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            pointer-events: none;
            color: rgba(0,0,0,0.8);
            font-weight: 500;
        }}
        .tooltip {{
            position: fixed;
            background: #2d2d30;
            border: 1px solid #3e3e3e;
            border-radius: 4px;
            padding: 8px 12px;
            font-size: 12px;
            pointer-events: none;
            z-index: 1000;
            max-width: 400px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }}
        .tooltip-name {{
            font-weight: bold;
            margin-bottom: 6px;
            color: #4fc3f7;
            font-size: 13px;
        }}
        .tooltip-time {{
            color: #9e9e9e;
            font-size: 11px;
            margin-bottom: 6px;
        }}
        .tooltip-attrs {{
            border-top: 1px solid #3e3e3e;
            padding-top: 6px;
            margin-top: 6px;
        }}
        .tooltip-attr {{
            margin: 2px 0;
            color: #ce9178;
        }}
        .tooltip-attr-key {{
            color: #9cdcfe;
        }}
        .controls-section {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .control-label {{
            font-size: 12px;
            color: #9e9e9e;
        }}
        button {{
            padding: 5px 10px;
            background: #3c3c3c;
            border: 1px solid #3e3e3e;
            color: #d4d4d4;
            border-radius: 3px;
            font-size: 12px;
            cursor: pointer;
        }}
        button:hover {{
            background: #4c4c4c;
        }}
    </style>
</head>
<body>
    <h1>LSM Trace Timeline</h1>
    <div id="controls">
        <div class="controls-section">
            <span class="control-label">Shard:</span>
            <select id="shard-selector"></select>
        </div>
        <div class="controls-section">
            <span class="control-label">Filter:</span>
            <button id="select-all">All</button>
            <button id="select-none">None</button>
        </div>
        <div id="filter-panel"></div>
        <span id="time-info"></span>
    </div>
    <div id="container">
        <div id="timeline"></div>
    </div>
    <div id="tooltip" class="tooltip" style="display: none;"></div>

    <script>
        const eventsData = {json.dumps(events_data)};
        const allEventNames = {json.dumps(all_event_names)};
        const minTime = {min_time};
        const maxTime = {max_time};
        const timeSpan = {time_span};

        let zoom = 1.0;
        let panX = 0;
        let isDragging = false;
        let dragStartX = 0;
        let dragStartPanX = 0;
        let currentShard = null;
        let visibleEvents = new Set(allEventNames);

        const container = document.getElementById('container');
        const timeline = document.getElementById('timeline');
        const tooltip = document.getElementById('tooltip');
        const timeInfo = document.getElementById('time-info');
        const shardSelector = document.getElementById('shard-selector');
        const filterPanel = document.getElementById('filter-panel');

        // Generate colors for event names
        const colors = {{}};
        function getColor(name) {{
            if (!colors[name]) {{
                const h = name.split('').reduce((acc, c) => acc + c.charCodeAt(0), 0) % 360;
                colors[name] = `hsl(${{h}}, 65%, 60%)`;
            }}
            return colors[name];
        }}

        function formatTime(us) {{
            const ms = us / 1000;
            if (ms < 1) return us.toFixed(0) + 'µs';
            if (ms < 1000) return ms.toFixed(2) + 'ms';
            return (ms / 1000).toFixed(3) + 's';
        }}

        function formatBytes(bytes) {{
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return (bytes / Math.pow(k, i)).toFixed(2) + ' ' + sizes[i];
        }}

        function formatAttrs(attrs) {{
            return Object.entries(attrs)
                .map(([k, v]) => {{
                    let formattedValue = v;
                    if (k.endsWith('_bytes') && typeof v === 'number') {{
                        formattedValue = formatBytes(v);
                    }} else if (typeof v === 'number') {{
                        formattedValue = v.toLocaleString();
                    }}
                    return `<div class="tooltip-attr"><span class="tooltip-attr-key">${{k}}</span>: ${{formattedValue}}</div>`;
                }})
                .join('');
        }}

        function initControls() {{
            // Populate shard selector
            const shards = Object.keys(eventsData).sort((a, b) => parseInt(a) - parseInt(b));
            shards.forEach(shard => {{
                const option = document.createElement('option');
                option.value = shard;
                option.textContent = `Shard ${{shard}}`;
                shardSelector.appendChild(option);
            }});
            currentShard = shards[0];
            shardSelector.value = currentShard;

            // Populate filter panel
            allEventNames.forEach(name => {{
                const color = getColor(name);
                const div = document.createElement('div');
                div.className = 'filter-item';
                div.innerHTML = `
                    <input type="checkbox" class="filter-checkbox" id="filter-${{name}}" checked data-event="${{name}}">
                    <label class="filter-label" for="filter-${{name}}">
                        <span class="color-box" style="background: ${{color}}"></span>
                        ${{name}}
                    </label>
                `;
                filterPanel.appendChild(div);
            }});

            // Event listeners
            shardSelector.addEventListener('change', (e) => {{
                currentShard = e.target.value;
                render();
            }});

            filterPanel.addEventListener('change', (e) => {{
                if (e.target.classList.contains('filter-checkbox')) {{
                    const eventName = e.target.dataset.event;
                    if (e.target.checked) {{
                        visibleEvents.add(eventName);
                    }} else {{
                        visibleEvents.delete(eventName);
                    }}
                    render();
                }}
            }});

            document.getElementById('select-all').addEventListener('click', () => {{
                visibleEvents = new Set(allEventNames);
                document.querySelectorAll('.filter-checkbox').forEach(cb => cb.checked = true);
                render();
            }});

            document.getElementById('select-none').addEventListener('click', () => {{
                visibleEvents.clear();
                document.querySelectorAll('.filter-checkbox').forEach(cb => cb.checked = false);
                render();
            }});
        }}

        function render() {{
            const containerWidth = container.clientWidth - 200;
            const timelineWidth = Math.max(containerWidth, containerWidth * zoom);

            // Group events by name for the current shard
            const eventsByName = {{}};
            if (eventsData[currentShard]) {{
                eventsData[currentShard].forEach(event => {{
                    if (!eventsByName[event.name]) {{
                        eventsByName[event.name] = [];
                    }}
                    eventsByName[event.name].push(event);
                }});
            }}

            let html = '';

            // Create a row for each event type
            for (const eventName of allEventNames) {{
                const isVisible = visibleEvents.has(eventName);
                const events = eventsByName[eventName] || [];
                const color = getColor(eventName);

                html += `<div class="event-row ${{isVisible ? '' : 'hidden'}}">`;
                html += `<div class="row-label" style="color: ${{color}}">${{eventName}}</div>`;
                html += `<div class="row-content">`;

                for (const event of events) {{
                    const startPx = ((event.start - minTime) / timeSpan) * timelineWidth + panX;
                    const endPx = ((event.end - minTime) / timeSpan) * timelineWidth + panX;
                    const widthPx = Math.max(endPx - startPx, event.type === 'instant' ? 3 : 2);

                    const duration = event.end - event.start;
                    const attrs = JSON.stringify(event.attrs);

                    html += `<div class="event ${{event.type}}" `;
                    html += `style="left: ${{startPx}}px; width: ${{widthPx}}px; background: ${{color}};" `;
                    html += `data-name="${{event.name}}" `;
                    html += `data-start="${{event.start}}" `;
                    html += `data-end="${{event.end}}" `;
                    html += `data-duration="${{duration}}" `;
                    html += `data-attrs='${{attrs}}'>`;
                    if (event.type === 'span' && widthPx > 50) {{
                        html += `<div class="event-label">${{formatTime(duration)}}</div>`;
                    }}
                    html += `</div>`;
                }}

                html += `</div>`;
                html += `</div>`;
            }}

            timeline.innerHTML = html;
            timeline.style.width = (timelineWidth + 200) + 'px';

            // Update time info
            const visibleCount = visibleEvents.size;
            timeInfo.textContent = `Total: ${{formatTime(timeSpan)}} | Zoom: ${{(zoom * 100).toFixed(0)}}% | Showing: ${{visibleCount}}/${{allEventNames.length}} types`;

            // Attach event listeners
            document.querySelectorAll('.event').forEach(el => {{
                el.addEventListener('mouseenter', showTooltip);
                el.addEventListener('mouseleave', hideTooltip);
            }});
        }}

        function showTooltip(e) {{
            const el = e.target;
            const name = el.dataset.name;
            const start = parseInt(el.dataset.start);
            const end = parseInt(el.dataset.end);
            const duration = parseInt(el.dataset.duration);
            const attrs = JSON.parse(el.dataset.attrs);

            let content = `<div class="tooltip-name">${{name}}</div>`;
            content += `<div class="tooltip-time">Start: ${{formatTime(start - minTime)}}</div>`;
            if (duration > 0) {{
                content += `<div class="tooltip-time">Duration: ${{formatTime(duration)}}</div>`;
            }}

            const attrsHtml = formatAttrs(attrs);
            if (attrsHtml) {{
                content += `<div class="tooltip-attrs">${{attrsHtml}}</div>`;
            }}

            tooltip.innerHTML = content;
            tooltip.style.display = 'block';
            updateTooltipPosition(e);
        }}

        function updateTooltipPosition(e) {{
            const x = e.clientX + 10;
            const y = e.clientY + 10;
            tooltip.style.left = x + 'px';
            tooltip.style.top = y + 'px';
        }}

        function hideTooltip() {{
            tooltip.style.display = 'none';
        }}

        // Zoom with mouse wheel
        container.addEventListener('wheel', (e) => {{
            e.preventDefault();
            const delta = e.deltaY > 0 ? 0.9 : 1.1;
            zoom = Math.max(0.1, Math.min(50, zoom * delta));
            render();
        }});

        // Pan with mouse drag
        container.addEventListener('mousedown', (e) => {{
            if (e.target === container || e.target === timeline || e.target.classList.contains('row-content')) {{
                isDragging = true;
                dragStartX = e.clientX;
                dragStartPanX = panX;
                container.style.cursor = 'grabbing';
            }}
        }});

        document.addEventListener('mousemove', (e) => {{
            if (isDragging) {{
                panX = dragStartPanX + (e.clientX - dragStartX);
                render();
            }}
        }});

        document.addEventListener('mouseup', () => {{
            isDragging = false;
            container.style.cursor = 'default';
        }});

        // Initialize and render
        initControls();
        render();
    </script>
</body>
</html>"""

    print(html_output)


if __name__ == "__main__":
    main()
