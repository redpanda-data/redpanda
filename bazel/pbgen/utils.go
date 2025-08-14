package main

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
)

func pascalToSnakeCase(s string) string {
	var builder strings.Builder
	// Iterate through the string character by character
	for i, r := range s {
		if !unicode.IsUpper(r) {
			builder.WriteRune(r)
			continue
		}
		if i == 0 {
			builder.WriteRune(unicode.ToLower(r))
			continue
		}

		prevRune := rune(s[i-1]) // Get the previous character for comparison

		// Condition 1: Transition from lowercase to uppercase (e.g., "pascalCase" -> "pascal_Case")
		isNewWordStart := unicode.IsLower(prevRune) && unicode.IsUpper(r)

		// Condition 2: Transition from an acronym to a new word (e.g., "RPCServer" -> "RPC_Server")
		// This occurs if the previous char was uppercase, current is uppercase, and the next is lowercase.
		isAcronymBoundary := unicode.IsUpper(prevRune) && unicode.IsUpper(r) &&
			i+1 < len(s) && unicode.IsLower(rune(s[i+1]))

		// Condition 3: Transition from a digit to an uppercase letter (e.g., "2023Foo" -> "2023_Foo")
		isDigitToUppercase := unicode.IsDigit(prevRune) && unicode.IsUpper(r)

		if isNewWordStart || isAcronymBoundary || isDigitToUppercase {
			builder.WriteRune('_')
		}
		builder.WriteRune(unicode.ToLower(r))
	}
	return builder.String()
}

// Sort a directed graph, possibly with cycles.
//
// This is done by using Tarjan's algorithm to find strongly connected components (SCCs),
// then we topologically sort each SCC.
//
// The returned order is where leaf nodes (nodes with no children) come first, then nodes
// that depend on them, and so on.
func sortCyclicalGraph[T comparable](nodes []T, getChildren func(node T) []T) []T {
	sccs := stronglyConnectedComponents(nodes, getChildren)
	nodeToSccID := make(map[T]int)
	for i, scc := range sccs {
		for _, node := range scc {
			nodeToSccID[node] = i
		}
	}
	// Build the graph of components.
	sccGraph := make(map[int][]int)
	sccNodeList := make([]int, len(sccs))
	for i := range sccs {
		sccNodeList[i] = i
		// For each node in the original graph...
		for _, node := range sccs[i] {
			// Look at its children...
			for _, child := range getChildren(node) {
				// If a child is in a *different* component, add an edge
				// between the components in our new graph.
				if nodeToSccID[node] != nodeToSccID[child] {
					sccGraph[nodeToSccID[node]] = append(sccGraph[nodeToSccID[node]], nodeToSccID[child])
				}
			}
		}
	}
	getSccChildren := func(sccID int) []int {
		return sccGraph[sccID]
	}
	sortedSccIndices, err := topologicalSort(sccNodeList, getSccChildren)
	if err != nil {
		panic(fmt.Errorf("stronglyConnectedComponents returned a cyclic graph, which is unexpected: %v", err))
	}
	var finalOrder []T
	for _, sccIndex := range sortedSccIndices {
		// The nodes within a cycle can be in any order, here we just append them.
		finalOrder = append(finalOrder, sccs[sccIndex]...)
	}
	slices.Reverse(finalOrder)
	return finalOrder
}

func stronglyConnectedComponents[T comparable](nodes []T, getChildren func(node T) []T) [][]T {
	state := &sccState[T]{
		nodes:       nodes,
		getChildren: getChildren,
		ids:         make(map[T]int),
		low:         make(map[T]int),
		onStack:     make(map[T]bool),
		stack:       make([]T, 0),
		generator:   0,
		sccs:        make([][]T, 0),
	}
	// The algorithm requires visiting each node. If a node hasn't been assigned an ID,
	// it means it hasn't been visited yet, so we start a DFS from it.
	for _, node := range state.nodes {
		if _, visited := state.ids[node]; !visited {
			state.dfs(node)
		}
	}
	return state.sccs
}

// sccState holds the state required for Tarjan's algorithm.
// It's used internally by the StronglyConnectedComponents function.
type sccState[T comparable] struct {
	nodes       []T
	getChildren func(node T) []T
	ids         map[T]int
	low         map[T]int
	onStack     map[T]bool
	stack       []T
	generator   int
	sccs        [][]T
}

// dfs is the core of Tarjan's algorithm. It performs a depth-first search
// to find the strongly connected components.
func (s *sccState[T]) dfs(at T) {
	// Push the current node onto the stack and mark it as on the stack.
	s.stack = append(s.stack, at)
	s.onStack[at] = true

	// Assign the node an ID and a low-link value from the generator.
	s.generator++
	s.ids[at] = s.generator
	s.low[at] = s.generator

	// Visit all neighbors of the current node.
	for _, to := range s.getChildren(at) {
		if _, visited := s.ids[to]; !visited {
			s.dfs(to)
		}
		// If the neighbor is on the stack, it's part of the current SCC.
		// We might need to update our low-link value.
		if s.onStack[to] {
			s.low[at] = min(s.low[at], s.low[to])
		}
	}
	// After visiting all neighbors, check if the current node is the root of an SCC.
	// The root is a node where its ID equals its low-link value.
	if s.ids[at] == s.low[at] {
		var scc []T
		// If it's a root, pop nodes from the stack until we pop the root itself.
		// All nodes popped here form one strongly connected component.
		for len(s.stack) > 0 {
			node := s.stack[len(s.stack)-1]
			s.stack = s.stack[:len(s.stack)-1]
			s.onStack[node] = false

			scc = append(scc, node)

			if node == at {
				break
			}
		}
		// Add the found SCC to our list of all SCCs.
		s.sccs = append(s.sccs, scc)
	}
}

// topologicalSort based on DFS
func topologicalSort[T comparable](nodes []T, getChildren func(node T) []T) ([]T, error) {
	var sortedOrder []T
	visited := make(map[T]bool)
	recursionStack := make(map[T]bool)
	var dfs func(node T) error
	dfs = func(node T) error {
		visited[node] = true
		recursionStack[node] = true
		for _, child := range getChildren(node) {
			if recursionStack[child] {
				return fmt.Errorf("graph contains a cycle")
			}
			if !visited[child] {
				if err := dfs(child); err != nil {
					return err
				}
			}
		}
		recursionStack[node] = false
		sortedOrder = append(sortedOrder, node)
		return nil
	}
	for _, node := range nodes {
		if !visited[node] {
			if err := dfs(node); err != nil {
				return nil, err
			}
		}
	}
	for i, j := 0, len(sortedOrder)-1; i < j; i, j = i+1, j-1 {
		sortedOrder[i], sortedOrder[j] = sortedOrder[j], sortedOrder[i]
	}
	return sortedOrder, nil
}
