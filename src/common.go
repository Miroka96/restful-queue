package main

type Data struct {
	Data string
}

type ListItem struct {
	Position int
	Data     Data
}

type Queue struct {
	Queue int
	Items []ListItem
}
