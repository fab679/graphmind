---
sidebar_position: 2
title: Architecture
description: System architecture and design overview
---

# Graphmind Graph Database - System Architecture

## Overview

This document describes the detailed architecture of Graphmind Graph Database with visual diagrams showing component interactions, data flows, and deployment models.

---

## 1. High-Level System Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        RC[Redis Client]
        HC[HTTP Client]
        GC[gRPC Client]
    end

    subgraph "Protocol Layer"
        RESP[RESP Protocol Handler]
        HTTP[HTTP API Handler]
        GRPC[gRPC Handler]
    end

    subgraph "Query Processing Layer"
        QP[Query Parser]
        QV[Query Validator]
        QPL[Query Planner]
        QO[Query Optimizer]
        QE[Query Executor]
    end

    subgraph "Storage Layer"
        GM[Graph Manager]
        NS[Node Store]
        ES[Edge Store]
        IS[Index Store]
        PS[Property Store]
    end

    subgraph "Persistence Layer"
        WAL[Write-Ahead Log]
        SS[Snapshot Manager]
        RDB[(RocksDB)]
    end

    subgraph "Infrastructure"
        MM[Memory Manager]
        TM[Transaction Manager]
        LM[Lock Manager]
        MT[Metrics & Tracing]
    end

    RC --> RESP
    HC --> HTTP
    GC --> GRPC

    RESP --> QP
    HTTP --> QP
    GRPC --> QP

    QP --> QV
    QV --> QPL
    QPL --> QO
    QO --> QE

    QE --> GM
    GM --> NS
    GM --> ES
    GM --> IS
    GM --> PS

    GM --> WAL
    WAL --> RDB
    SS --> RDB

    GM --> MM
    GM --> TM
    TM --> LM
    GM --> MT


```

---

## 2. Component Architecture Details

### 2.1 Protocol Handler Layer

```mermaid
sequenceDiagram
    participant C as Client
    participant R as RESP Handler
    participant D as Dispatcher
    participant Q as Query Engine
    participant S as Storage

    C->>R: GRAPH.QUERY mygraph "MATCH (n) RETURN n"
    R->>R: Parse RESP command
    R->>D: Dispatch(GRAPH.QUERY, args)
    D->>Q: ExecuteQuery(graph, query)
    Q->>Q: Parse → Plan → Optimize
    Q->>S: Execute(plan)
    S-->>Q: Results
    Q-->>D: QueryResult
    D-->>R: Response
    R->>R: Encode RESP response
    R-->>C: RESP Array of results
```

**RESP Command Flow**:
```mermaid
stateDiagram-v2
    [*] --> ReadCommand
    ReadCommand --> ParseCommand
    ParseCommand --> ValidateAuth
    ValidateAuth --> Dispatch
    Dispatch --> ExecuteRead: Read Query
    Dispatch --> ExecuteWrite: Write Query
    ExecuteRead --> EncodeResponse
    ExecuteWrite --> WAL
    WAL --> UpdateMemory
    UpdateMemory --> EncodeResponse
    EncodeResponse --> SendResponse
    SendResponse --> [*]
```

### 2.2 Query Processing Pipeline

```mermaid
graph LR
    subgraph "Parsing Phase"
        CQ[Cypher Query] --> LEX[Lexer]
        LEX --> PAR[Parser]
        PAR --> AST[Abstract Syntax Tree]
    end

    subgraph "Validation Phase"
        AST --> SEM[Semantic Analyzer]
        SEM --> TYPE[Type Checker]
        TYPE --> VAST[Validated AST]
    end

    subgraph "Planning Phase"
        VAST --> LP[Logical Planner]
        LP --> LPLAN[Logical Plan]
        LPLAN --> PP[Physical Planner]
        PP --> PPLAN[Physical Plan]
    end

    subgraph "Optimization Phase"
        PPLAN --> RBO[Rule-Based Optimizer]
        RBO --> CBO[Cost-Based Optimizer]
        CBO --> OPLAN[Optimized Plan]
    end

    subgraph "Execution Phase"
        OPLAN --> EXE[Executor]
        EXE --> RES[Results]
    end

```

**Example Query Execution Plan**:
```mermaid
graph TD
    A[Project: b.name] --> B[Filter: a.age > 30]
    B --> C[Expand: KNOWS edge]
    C --> D[NodeScan: Person label]

```

### 2.3 Storage Engine Architecture

```mermaid
graph TB
    subgraph "In-Memory Graph Store"
        direction TB
        NM[Node Manager]
        EM[Edge Manager]
        PM[Property Manager]
        IM[Index Manager]

        subgraph "Node Storage"
            NH[Node HashMap<br/>NodeId → Node]
            NL[Label Index<br/>Label → NodeIds]
        end

        subgraph "Edge Storage"
            EH[Edge HashMap<br/>EdgeId → Edge]
            ADJ_OUT[Outgoing Adjacency<br/>NodeId → EdgeIds]
            ADJ_IN[Incoming Adjacency<br/>NodeId → EdgeIds]
        end

        subgraph "Property Storage"
            PC[Property Columns<br/>Columnar Storage]
            PI[Property Index<br/>Hash/BTree]
        end

        subgraph "Indices"
            LI[Label Index<br/>RoaringBitmap]
            PRI[Property Index<br/>RoaringBitmap]
        end

        NM --> NH
        NM --> NL
        EM --> EH
        EM --> ADJ_OUT
        EM --> ADJ_IN
        PM --> PC
        PM --> PI
        IM --> LI
        IM --> PRI
    end

    subgraph "Persistence Layer"
        RDB[(RocksDB)]
        CF1[Column Family: nodes]
        CF2[Column Family: edges]
        CF3[Column Family: wal]
        CF4[Column Family: metadata]

        RDB --> CF1
        RDB --> CF2
        RDB --> CF3
        RDB --> CF4
    end

    NM -.->|Persist| CF1
    EM -.->|Persist| CF2
    PM -.->|WAL| CF3

```

**Data Structures**:
```mermaid
classDiagram
    class Node {
        +NodeId id
        +Vec~Label~ labels
        +PropertyMap properties
        +Timestamp created_at
        +Timestamp updated_at
    }

    class Edge {
        +EdgeId id
        +NodeId source
        +NodeId target
        +EdgeType edge_type
        +PropertyMap properties
        +Timestamp created_at
    }

    class PropertyMap {
        +HashMap~String,i64~ int_props
        +HashMap~String,f64~ float_props
        +HashMap~String,String~ string_props
        +HashMap~String,bool~ bool_props
    }

    class GraphStore {
        +HashMap~NodeId,Node~ nodes
        +HashMap~EdgeId,Edge~ edges
        +HashMap~NodeId,Vec~EdgeId~~ outgoing
        +HashMap~NodeId,Vec~EdgeId~~ incoming
        +IndexStore indices
    }

    class IndexStore {
        +HashMap~Label,RoaringBitmap~ label_index
        +HashMap~PropertyKey,PropertyIndex~ property_index
    }

    Node --> PropertyMap
    Edge --> PropertyMap
    GraphStore --> Node
    GraphStore --> Edge
    GraphStore --> IndexStore
```

### 2.4 Memory Management

```mermaid
graph TB
    subgraph "Memory Tiers"
        HOT[Hot Tier<br/>In-Memory<br/>Recently Accessed]
        WARM[Warm Tier<br/>Memory-Mapped<br/>Occasionally Accessed]
        COLD[Cold Tier<br/>Disk Only<br/>Rarely Accessed]
    end

    subgraph "Memory Manager"
        ALLOC[Custom Allocator]
        POOL[Memory Pools]
        EVICT[Eviction Policy<br/>LRU/LFU]
    end

    subgraph "Monitoring"
        MEM_MON[Memory Monitor]
        QUOTA[Quota Enforcer]
        GC[Compaction]
    end

    HOT -->|Access Pattern| EVICT
    EVICT -->|Evict Cold| WARM
    WARM -->|Evict Coldest| COLD
    COLD -->|Promote Hot| HOT

    ALLOC --> POOL
    POOL --> HOT

    MEM_MON --> QUOTA
    QUOTA -->|Limit Exceeded| EVICT
    MEM_MON -->|Fragmentation| GC

```

---

## 3. Distributed Architecture (Phase 3+)

### 3.1 Cluster Topology

```mermaid
graph TB
    subgraph "3-Node Raft Cluster"
        L[Leader Node<br/>Handles Writes]
        F1[Follower Node 1<br/>Read Replica]
        F2[Follower Node 2<br/>Read Replica]
    end

    subgraph "Clients"
        C1[Client 1]
        C2[Client 2]
        C3[Client 3]
    end

    C1 -->|Write| L
    C2 -->|Read| F1
    C3 -->|Read| F2

    L -.->|Replicate| F1
    L -.->|Replicate| F2
    F1 -.->|Heartbeat| L
    F2 -.->|Heartbeat| L

```

### 3.2 Raft Consensus Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: Write Request
    L->>L: Append to local log
    par Replicate to Followers
        L->>F1: AppendEntries RPC
        L->>F2: AppendEntries RPC
    end

    F1->>F1: Append to log
    F2->>F2: Append to log

    F1-->>L: Success
    F2-->>L: Success

    Note over L: Majority achieved (2/3)

    L->>L: Commit entry
    L->>L: Apply to state machine
    L-->>C: Success Response

    par Notify Followers
        L->>F1: Commit index
        L->>F2: Commit index
    end

    F1->>F1: Apply to state machine
    F2->>F2: Apply to state machine
```

### 3.3 Leader Election

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: Election timeout
    Candidate --> Leader: Receives majority votes
    Candidate --> Follower: Discovers leader or new term
    Leader --> Follower: Discovers higher term
    Follower --> Follower: Receives heartbeat

    note right of Follower
        Responds to RPCs
        Forwards writes to leader
    end note

    note right of Candidate
        Requests votes
        Increments term
    end note

    note right of Leader
        Handles writes
        Sends heartbeats
        Replicates log
    end note
```

---

## 4. Query Execution Architecture

### 4.1 Volcano Iterator Model

```mermaid
graph TB
    subgraph "Query Plan Tree"
        PROJ[ProjectOperator<br/>SELECT b.name]
        FILT[FilterOperator<br/>WHERE a.age > 30]
        EXP[ExpandOperator<br/>-[:KNOWS]->]
        SCAN[NodeScanOperator<br/>MATCH :Person]
    end

    PROJ --> FILT
    FILT --> EXP
    EXP --> SCAN

```

### 4.2 Query Optimization

```mermaid
graph TB
    subgraph "Rule-Based Optimization"
        R1[Predicate Pushdown]
        R2[Index Selection]
        R3[Join Reordering]
        R4[Constant Folding]
    end

    subgraph "Cost-Based Optimization"
        C1[Statistics Collection]
        C2[Cardinality Estimation]
        C3[Cost Model]
        C4[Plan Enumeration]
    end

    LP[Logical Plan] --> R1
    R1 --> R2
    R2 --> R3
    R3 --> R4
    R4 --> OLP[Optimized Logical Plan]

    OLP --> C1
    C1 --> C2
    C2 --> C3
    C3 --> C4
    C4 --> PP[Physical Plan]

```

---

## 5. Multi-Tenancy Architecture

```mermaid
graph TB
    subgraph "Tenant Isolation"
        T1[Tenant 1 Namespace]
        T2[Tenant 2 Namespace]
        T3[Tenant 3 Namespace]
    end

    subgraph "Shared Infrastructure"
        QE[Query Engine]
        SE[Storage Engine]
        PE[Persistence]
    end

    subgraph "Resource Control"
        QM[Quota Manager]
        RM[Resource Monitor]
        RL[Rate Limiter]
    end

    T1 --> QM
    T2 --> QM
    T3 --> QM

    QM --> RL
    RL --> QE
    QE --> SE
    SE --> PE

    RM --> QM

```

---

## 6. Persistence and Recovery

### 6.1 Write-Ahead Log (WAL)

```mermaid
sequenceDiagram
    participant C as Client
    participant E as Executor
    participant W as WAL
    participant M as Memory
    participant D as Disk (RocksDB)

    C->>E: Write Operation
    E->>W: Append to WAL
    W->>D: fsync() [if sync mode]
    D-->>W: Persisted

    W->>M: Update in-memory
    M-->>E: Success
    E-->>C: Acknowledge

    Note over W,D: Background: Checkpoint<br/>WAL to RocksDB

    loop Periodic
        W->>D: Flush to RocksDB
        W->>W: Truncate old WAL
    end
```

### 6.2 Recovery Process

```mermaid
graph TB
    START[Database Start] --> CHECK[Check for snapshot]

    CHECK -->|Found| LOAD[Load Latest Snapshot]
    CHECK -->|Not Found| EMPTY[Start Empty]

    LOAD --> REPLAY[Replay WAL from snapshot point]
    EMPTY --> REPLAY

    REPLAY --> REBUILD[Rebuild Indices]
    REBUILD --> VALIDATE[Validate Integrity]
    VALIDATE --> READY[Ready to Serve]

```

---

## 7. Deployment Architecture

### 7.1 Single-Node Deployment

```mermaid
graph TB
    subgraph "Docker Container"
        SRV[Graphmind Server]
        RDB[(RocksDB Data)]
        CFG[Config Files]
        LOG[Logs]
    end

    subgraph "Host Machine"
        VOL1[/data Volume]
        VOL2[/config Volume]
        VOL3[/logs Volume]
    end

    RDB -.->|Mount| VOL1
    CFG -.->|Mount| VOL2
    LOG -.->|Mount| VOL3

    LB[Load Balancer] --> SRV

    CLIENT1[Client 1] --> LB
    CLIENT2[Client 2] --> LB
    CLIENT3[Client 3] --> LB

```

### 7.2 Kubernetes Deployment (HA Cluster)

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "Graphmind StatefulSet"
            POD1[graphmind-0<br/>Leader]
            POD2[graphmind-1<br/>Follower]
            POD3[graphmind-2<br/>Follower]
        end

        subgraph "Persistent Volumes"
            PV1[(PV: graphmind-0)]
            PV2[(PV: graphmind-1)]
            PV3[(PV: graphmind-2)]
        end

        SVC[Service<br/>Load Balancer]

        POD1 --> PV1
        POD2 --> PV2
        POD3 --> PV3

        SVC --> POD1
        SVC --> POD2
        SVC --> POD3
    end

    INGRESS[Ingress] --> SVC

```

---

## Summary

This architecture provides:

- **Modularity**: Clear separation of concerns
- **Scalability**: From single-node to distributed cluster
- **Performance**: Multi-tier caching, optimized data structures
- **Reliability**: WAL, snapshots, Raft consensus
- **Observability**: Comprehensive metrics, tracing, logging
- **Security**: Multi-layer security architecture
- **Flexibility**: Multiple protocols, deployment options

**Key Design Principles**:
1. **Start Simple**: Single-node first, distribute later
2. **Optimize for Reads**: In-memory caching, indices
3. **Durability First**: WAL before acknowledgment
4. **Fail-Safe**: Raft quorum, split-brain prevention
5. **Observable**: Metrics and traces at every layer
