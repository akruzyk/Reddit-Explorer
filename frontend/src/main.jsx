import React from "react";
import ReactDOM from "react-dom/client";
import RedditExplorer from "./components/RedditExplorer.jsx";
import "./styles/tailwind.css";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <RedditExplorer />
  </React.StrictMode>
);
