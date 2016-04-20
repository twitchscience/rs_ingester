package loadclient

type loadError struct {
	msg         string
	isRetryable bool
}

func (e loadError) Error() string {
	return e.msg
}

func (e loadError) Retryable() bool {
	return e.isRetryable
}

type entry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type manifest struct {
	Entries []entry `json:"entries"`
}
