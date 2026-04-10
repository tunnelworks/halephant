import { defineConfig } from "vitepress"
import {
  groupIconMdPlugin,
  groupIconVitePlugin,
} from "vitepress-plugin-group-icons"

// https://vitepress.dev/reference/site-config
export default defineConfig({
  srcDir: "src",
  base: process.env.VITEPRESS_BASE || "/",

  title: "Halephant",
  description: "Connection pooling for PostgreSQL",

  markdown: {
    config(md) {
      md.use(groupIconMdPlugin)
    },
  },

  vite: {
    plugins: [
      groupIconVitePlugin({
        customIcon: {
          ".toml": "vscode-icons:file-type-toml",
          halephant: "vscode-icons:file-type-toml",
        },
      }),
    ],
  },

  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: "Home", link: "/" },
      { text: "Guide", link: "/guide/configuration" },
      { text: "Clients", link: "/clients/python/psycopg" },
    ],

    sidebar: [
      {
        text: "Guide",
        items: [
          { text: "Configuration", link: "/guide/configuration" },
          { text: "Read replica load balancing", link: "/guide/read-replicas" },
          { text: "Backpressure and queueing", link: "/guide/backpressure" },
          { text: "LISTEN/NOTIFY", link: "/guide/listen-notify" },
          { text: "OpenTelemetry", link: "/guide/otel" },
        ],
      },
      {
        text: "Clients",
        items: [
          {
            text: "Python",
            collapsed: true,
            items: [
              { text: "psycopg3", link: "/clients/python/psycopg" },
              { text: "asyncpg", link: "/clients/python/asyncpg" },
              { text: "SQLAlchemy", link: "/clients/python/sqlalchemy" },
              { text: "Django", link: "/clients/python/django" },
            ],
          },
          {
            text: "Ruby",
            collapsed: true,
            items: [
              { text: "Rails", link: "/clients/ruby/rails" },
            ],
          },
          {
            text: "PHP",
            collapsed: true,
            items: [
              { text: "Laravel", link: "/clients/php/laravel" },
            ],
          },
          {
            text: "Java",
            collapsed: true,
            items: [
              { text: "JDBC", link: "/clients/java/jdbc" },
            ],
          },
          {
            text: "Go",
            collapsed: true,
            items: [
              { text: "pgx", link: "/clients/go/pgx" },
            ],
          },
          {
            text: "Rust",
            collapsed: true,
            items: [
              { text: "tokio-postgres", link: "/clients/rust/tokio-postgres" },
              { text: "sqlx", link: "/clients/rust/sqlx" },
            ],
          },
          {
            text: "Node.js",
            collapsed: true,
            items: [
              { text: "Prisma", link: "/clients/node/prisma" },
              { text: "node-postgres", link: "/clients/node/pg" },
            ],
          },
        ],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/tunnelworks/halephant" },
    ],
  },
})
