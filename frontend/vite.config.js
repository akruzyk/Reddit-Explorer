import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "./dist", // Flask will serve from here
    assetsDir: "assets",
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:5001",
    },
  },
});
