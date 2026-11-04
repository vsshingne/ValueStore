import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
export default defineConfig({plugins:[react(),tailwindcss()],build:{rollupOptions:{output:{manualChunks:{charts:['recharts'],react:['react','react-dom','react-router-dom']}}}},server:{proxy:{'/api':process.env.API_PROXY_TARGET||'http://127.0.0.1:8080'}}});
