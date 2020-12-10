import React from "react";
import "./IconButton.scss";

import collapse from "assets/images/ui-icons/minimize-2.svg";
import expand from "assets/images/ui-icons/maximize-2.svg";
import add from "assets/images/ui-icons/plus.svg";
import remove from "assets/images/ui-icons/x.svg";
import reset from "assets/images/ui-icons/reset.svg";

const iconTypes = {
  add: add,
  remove: remove,
  reset: reset,
  expand: expand,
  collapse: collapse,
};

const IconButton = ({
  type,
  text,
  ...props
}: {
  type: keyof typeof iconTypes;
  text?: string;
  [x: string]: any;
}) => {
  return (
    <button className="IconButton" {...props}>
      <img
        src={iconTypes[type]}
        alt={type}
        style={{ width: "100%", height: "100%" }}
      />
      {text && <span>{text}</span>}
    </button>
  );
};

export default IconButton;
