# Graphmind Optimization

Metaheuristic optimization solver library for [Graphmind](https://github.com/fab679/graphmind).

## Solvers

- **Genetic Algorithm (GA)**
- **Particle Swarm Optimization (PSO)**
- **Differential Evolution (DE)**
- **Simulated Annealing (SA)**
- **Teaching-Learning Based Optimization (TLBO)**
- **JAYA** — Parameter-free optimization
- **Grey Wolf Optimizer (GWO)**
- **Artificial Bee Colony (ABC)**
- **Firefly Algorithm**
- **Bat Algorithm**
- **Cuckoo Search**
- **Flower Pollination Algorithm**
- **Harmony Search**
- **Gravitational Search Algorithm (GSA)**
- **NSGA-II** — Multi-objective optimization
- **Rao Algorithms** (Rao-1, Rao-2, Rao-3)

## Usage

```rust
use graphmind_optimization::{PsoSolver, OptimizationConfig};

let config = OptimizationConfig::default();
let result = PsoSolver::new(config).solve(&objective_fn);
```

## License

Apache-2.0
