
console.log('Hello World')

const eventSource = new EventSource('http://localhost:8081/stream/data');

eventSource.onopen = (x) => {
  console.log("SSE onopen", x);
}

eventSource.onmessage = event => {
  console.log('SSE onmessage:', event.data);
};

eventSource.onerror = error => {
  console.error('SSE onerror:', error);
};

