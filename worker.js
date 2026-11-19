export default {
  async fetch(request) {
    return new Response('Привет, воркер живой!', { status: 200 });
  },
};
