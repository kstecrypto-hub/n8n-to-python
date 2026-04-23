import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({
    base: "/app/",
    plugins: [react()],
    resolve: {
        alias: {
            "@": "/src",
        },
    },
    server: {
        host: "127.0.0.1",
        port: 3000,
        strictPort: true,
        proxy: {
            "/auth": "http://127.0.0.1:38100",
            "/agent": "http://127.0.0.1:38100",
            "/admin": "http://127.0.0.1:38100",
            "/health": "http://127.0.0.1:38100",
            "/ingest": "http://127.0.0.1:38100",
            "/places": "http://127.0.0.1:38100",
            "/hives": "http://127.0.0.1:38100",
            "/sensors": "http://127.0.0.1:38100",
        },
    },
    preview: {
        host: "127.0.0.1",
        port: 3001,
        strictPort: true,
    },
});
