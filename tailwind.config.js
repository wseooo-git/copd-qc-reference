/** @type {import('tailwindcss').Config} */
export default {
    content: [
        "./index.html",
        "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
        extend: {
            colors: {
                primary: '#1976D2',
                success: '#4CAF50',
                warning: '#FF9800',
                error: '#F44336',
                info: '#2196F3',
            }
        },
    },
    plugins: [],
}
