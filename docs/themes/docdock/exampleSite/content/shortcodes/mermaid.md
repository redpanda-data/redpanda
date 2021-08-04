+++
title = "mermaid"
description = ""
+++

## Flowchart example
{{%expand "Show code ..."%}}
	{{</*mermaid align="left"*/>}}
	graph LR;
		A[Hard edge] -->|Link text| B(Round edge)
    	B --> C{Decision}
    	C -->|One| D[Result one]
    	C -->|Two| E[Result two]
    {{</* /mermaid */>}}
{{%/expand%}}

{{<mermaid align="left">}}
graph LR;
	A[Hard edge] -->|Link text| B(Round edge)
    B --> C{Decision}
    C -->|One| D[Result one]
    C -->|Two| E[Result two]
{{< /mermaid >}}

### With sub-graphs and some style

{{%expand "Show code..."%}}
    {{</*mermaid align="left"*/>}}
    graph LR;
        X --> Y
        linkStyle 0 stroke:#f00,stroke-width:4px;
        Y --> Z
        Z --> X
        linkStyle 1,2 interpolate basis stroke:#0f0,stroke-width:2px;
        X --> A1
        subgraph right
            A2 --> B2
            B2 --> C2
        end
        subgraph left
            A1 --> B1
            B1 --> C1
        end
        C1 --> X
        Z --> A2
        C2 --> Z

        style Y fill:#f9f,stroke:#333,stroke-width:4px

        classDef left fill:#ccf,stroke:#f66,stroke-width:2px,stroke-dasharray: 5, 5
        class A1,B1,C1 left
    {{</* /mermaid */>}}
{{%/expand%}}

{{<mermaid align="left">}}
graph LR;
    X --> Y
    linkStyle 0 stroke:#f00,stroke-width:4px;
    Y --> Z
    Z --> X
    linkStyle 1,2 interpolate basis stroke:#0f0,stroke-width:2px;
    X --> A1
    subgraph right
        A2 --> B2
        B2 --> C2
    end
    subgraph left
        A1 --> B1
        B1 --> C1
    end
    C1 --> X
    Z --> A2
    C2 --> Z

    style Y fill:#f9f,stroke:#333,stroke-width:4px

    classDef left fill:#ccf,stroke:#f66,stroke-width:2px,stroke-dasharray: 5, 5
    class A1,B1,C1 left
{{</mermaid>}}

## Sequence example
{{%expand "Show code ..."%}}
	{{</*mermaid*/>}}
	sequenceDiagram
	    participant Alice
	    participant Bob
	    Alice->>John: Hello John, how are you?
	    loop Healthcheck
	        John->John: Fight against hypochondria
	    end
	    Note right of John: Rational thoughts <br/>prevail...
	    John-->Alice: Great!
	    John->Bob: How about you?
	    Bob-->John: Jolly good!
	{{</* /mermaid */>}}
{{%/expand%}}

{{<mermaid>}}
sequenceDiagram
    participant Alice
    participant Bob
    Alice->>John: Hello John, how are you?
    loop Healthcheck
        John->John: Fight against hypochondria
    end
    Note right of John: Rational thoughts <br/>prevail...
    John-->Alice: Great!
    John->Bob: How about you?
    Bob-->John: Jolly good!
{{< /mermaid >}}



## GANTT Example
{{%expand "Show code ..."%}}
	{{</*mermaid*/>}}
	gantt
	        dateFormat  YYYY-MM-DD
	        title Adding GANTT diagram functionality to mermaid
	        section A section
	        Completed task            :done,    des1, 2014-01-06,2014-01-08
	        Active task               :active,  des2, 2014-01-09, 3d
	        Future task               :         des3, after des2, 5d
	        Future task2               :         des4, after des3, 5d
	        section Critical tasks
	        Completed task in the critical line :crit, done, 2014-01-06,24h
	        Implement parser and jison          :crit, done, after des1, 2d
	        Create tests for parser             :crit, active, 3d
	        Future task in critical line        :crit, 5d
	        Create tests for renderer           :2d
	        Add to mermaid                      :1d
	{{</* /mermaid */>}}
{{%/expand%}}

{{<mermaid>}}
gantt
        dateFormat  YYYY-MM-DD
        title Adding GANTT diagram functionality to mermaid
        section A section
        Completed task            :done,    des1, 2014-01-06,2014-01-08
        Active task               :active,  des2, 2014-01-09, 3d
        Future task               :         des3, after des2, 5d
        Future task2               :         des4, after des3, 5d
        section Critical tasks
        Completed task in the critical line :crit, done, 2014-01-06,24h
        Implement parser and jison          :crit, done, after des1, 2d
        Create tests for parser             :crit, active, 3d
        Future task in critical line        :crit, 5d
        Create tests for renderer           :2d
        Add to mermaid                      :1d
{{</mermaid>}}



